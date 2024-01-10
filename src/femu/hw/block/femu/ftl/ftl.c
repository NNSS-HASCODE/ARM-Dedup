#include "qemu/osdep.h"
#include "hw/block/block.h"
#include "hw/pci/msix.h"
#include "hw/pci/msi.h"
#include "../nvme.h"
#include "ftl.h"
#include <execinfo.h>

uint16_t ssd_count = 0;

static void *ftl_thread(void *arg);

/* wen:added  Remap-SSD */
void printf_info(struct ssd *ssd, bool force_print);     //32 Lines
uint64_t ppa_to_OffsetInLine(struct ssd *ssd, struct ppa *ppa);  //6 Lines
struct ppa OffsetInLine_to_ppa(struct ssd *ssd, uint64_t offset, uint64_t line_id);  //14 Lines
struct RMM* get_RMM(struct ssd *ssd, struct line* line); //24 Lines
struct RMM_page *get_RMM_page(struct ssd *ssd);      //6 Lines
bool is_valid_RMM(struct ssd *ssd, struct RMM *RMM, int line_id);    //10 Lines
void ssd_init_remote_maptbl(struct ssd *ssd, uint64_t R_MapTable_size);     //10 Lines
void ssd_init_segments(struct ssd *ssd);    //12 Lines
void ssd_init_write_RMM_pointer(struct ssd *ssd);   //18 Lines
void ssd_init_GC_migration_mappins(struct ssd *ssd);     //7 Lines
void segmentgroup_migration(struct ssd *ssd, struct line *line);    //44 Lines
uint64_t ssd_remote_read(struct ssd *ssd, NvmeRequest *req);    //18 Lines
uint64_t ssd_dedup_write(struct ssd *ssd, NvmeRequest *req);    //56 Lines
uint64_t ssd_remote_parity_write(struct ssd *ssd, NvmeRequest *req);    
uint64_t ssd_trim(struct ssd *ssd, NvmeRequest *req);   //64 Lines

/* 打印调用栈的最大深度 */
#define DUMP_STACK_DEPTH_MAX 16
/* 打印调用栈函数 */
void dump_trace(struct ssd* ssd) {
    void *stack_trace[DUMP_STACK_DEPTH_MAX] = {0};
    char **stack_strings = NULL;
    int stack_depth = 0;
    int i = 0;

    FILE *fp = fopen(ssd->info_file_name, "a");

    /* 获取栈中各层调用函数地址 */
    stack_depth = backtrace(stack_trace, DUMP_STACK_DEPTH_MAX);
    /* 查找符号表将函数调用地址转换为函数名称 */
    stack_strings = (char **)backtrace_symbols(stack_trace, stack_depth);
    if (NULL == stack_strings) {
        fprintf(fp, " Memory is not enough while dump Stack Trace! \r\n");
        return;
    }
    /* 打印调用栈 */
    fprintf(fp, " Stack Trace: \r\n");
    for (i = 0; i < stack_depth; ++i) {
        fprintf(fp, " [%d] %s \r\n", i, stack_strings[i]);
    }
    /* 获取函数名称时申请的内存需要自行释放 */
    free(stack_strings);
    stack_strings = NULL;
    fclose(fp);
    return;
}

static inline unsigned long read_tsc(void) {
    unsigned long var;
    unsigned int hi, lo;

    asm volatile ("rdtsc" : "=a" (lo), "=d" (hi));
    var = ((unsigned long long int) hi << 32) | lo;

    return var;
}

static inline bool should_gc(struct ssd *ssd)
{
    return (ssd->lm.free_line_cnt <= ssd->sp.gc_thres_lines);
}

static inline bool should_gc_high(struct ssd *ssd)
{
    return (ssd->lm.free_line_cnt < ssd->sp.gc_thres_lines_high);
}

inline struct ppa get_maptbl_ent(struct ssd *ssd, uint64_t lpn, bool if_remote_lpn)
{
    my_assert(ssd, lpn != RMM_PAGE, "error, there is no maptbl for RMM_PAGE\n");
    if(if_remote_lpn)                    return ssd->remote_maptbl[lpn];
    else if(lpn >= ssd->sp.tt_pgs)       return ssd->remote_maptbl[lpn % ssd->sp.tt_pgs];   //remote parity
    else                                 return ssd->maptbl[lpn];
}

static inline void set_maptbl_ent(struct ssd *ssd, uint64_t lpn, bool if_remote_lpn, struct ppa *ppa)
{
    if (lpn == RMM_PAGE) 
        return;
    
    my_assert(ssd, lpn != INVALID_LPN, "Error, it is an INVALID_LPN in set_maptbl_ent()");

    if (if_remote_lpn)                  ssd->remote_maptbl[lpn] = *ppa;
    else if(lpn >= ssd->sp.tt_pgs)      ssd->remote_maptbl[lpn % ssd->sp.tt_pgs] = *ppa;
    else                                ssd->maptbl[lpn] = *ppa;
}

static uint64_t ppa2pgidx(struct ssd *ssd, struct ppa *ppa)
{
    struct ssdparams *spp = &ssd->sp;
    uint64_t pgidx;

    pgidx = ppa->g.ch * spp->pgs_per_ch + ppa->g.lun * spp->pgs_per_lun +
        ppa->g.pl * spp->pgs_per_pl + ppa->g.blk * spp->pgs_per_blk + ppa->g.pg;

    my_assert(ssd, pgidx < spp->tt_pgs, "Error, pgidx(%d) is larger than spp->tt_pgs(%d), ch(%d), lun(%d), pl(%d), blk(%d), pg(%d)", 
            pgidx, spp->tt_pgs, ppa->g.ch, ppa->g.lun, ppa->g.pl, ppa->g.blk, ppa->g.pg);

    return pgidx;
}

inline struct rmap_elem get_rmap_ent(struct ssd *ssd, struct ppa *ppa)
{
    uint64_t pgidx = ppa2pgidx(ssd, ppa);

    return ssd->rmap[pgidx];
}

/* set rmap[page_no(ppa)] -> lpn */
static inline void set_rmap_ent(struct ssd *ssd, struct rmap_elem elem, struct ppa *ppa)
{
    uint64_t pgidx = ppa2pgidx(ssd, ppa);

    ssd->rmap[pgidx] = elem;
}

static void ssd_init_lines(struct ssd *ssd)
{
    int i;
    struct ssdparams *spp = &ssd->sp;
    struct line_mgmt *lm = &ssd->lm;
    struct line *line;

    lm->tt_lines = spp->blks_per_pl;
    assert(lm->tt_lines == spp->tt_lines);
    lm->lines = g_malloc0(sizeof(struct line) * lm->tt_lines);

    QTAILQ_INIT(&lm->free_line_list);
    QTAILQ_INIT(&lm->victim_line_list);
    QTAILQ_INIT(&lm->full_line_list);

    lm->free_line_cnt = 0;
    for (i = 0; i < lm->tt_lines; i++) {
        line = &lm->lines[i];
        line->id = i;
        line->ipc = 0;
        line->vpc = 0;
        /* initialize all the lines as free lines */
        QTAILQ_INSERT_TAIL(&lm->free_line_list, line, entry);
        lm->free_line_cnt++;

        /* wen:added  Remap-SSD */
        QTAILQ_INIT(&line->segment_group_list);
        QTAILQ_INIT(&line->RMM_page_list);
        line->is_RMM_line = false;
        line->segment_count_inNVRAM = 0;
        line->next_segment_id = 0;
    }

    assert(lm->free_line_cnt == lm->tt_lines);
    lm->victim_line_cnt = 0;
    lm->full_line_cnt = 0;
}

static void ssd_init_write_pointer(struct ssd *ssd)
{
    struct write_pointer *wpp = &ssd->wp;
    struct line_mgmt *lm = &ssd->lm;
    struct line *curline = NULL;
    /* make sure lines are already initialized by now */
    curline = QTAILQ_FIRST(&lm->free_line_list);
    QTAILQ_REMOVE(&lm->free_line_list, curline, entry);
    lm->free_line_cnt--;
    /* wpp->curline is always our onging line for writes */
    wpp->curline = curline;
    wpp->ch = 0;
    wpp->lun = 0;
    wpp->pg = 0;
    wpp->blk = curline->id;
    wpp->pl = 0;
}

static inline void check_addr(int a, int max)
{
    assert(a >= 0 && a < max);
}

static struct line *get_next_free_line(struct ssd *ssd, bool is_RMM_page)
{
    struct line_mgmt *lm = &ssd->lm;
    struct line *curline = NULL;

    curline = QTAILQ_FIRST(&lm->free_line_list);
    if (!curline) {
        printf("FEMU-FTL: Error, there is no free lines left in [%s] !!!!\n", ssd->ssdname);
        return NULL;
    }

    QTAILQ_REMOVE(&lm->free_line_list, curline, entry);
    lm->free_line_cnt--;
    /* wen:added  Remap-SSD */
    curline->is_RMM_line = is_RMM_page;
    return curline;
}

static void ssd_advance_write_pointer(struct ssd *ssd, bool is_RMM_page)
{
    struct ssdparams *spp = &ssd->sp;
    struct write_pointer *wpp = is_RMM_page ? &ssd->wp_RMM : &ssd->wp;
    struct line_mgmt *lm = &ssd->lm;

    check_addr(wpp->ch, spp->nchs);
    wpp->ch++;
    if (wpp->ch == spp->nchs) {
        wpp->ch = 0;
        check_addr(wpp->lun, spp->luns_per_ch);
        wpp->lun++;
        /* in this case, we should go to next lun */
        if (wpp->lun == spp->luns_per_ch) {
            wpp->lun = 0;
            /* go to next page in the block */
            check_addr(wpp->pg, spp->pgs_per_blk);
            wpp->pg++;
            if (wpp->pg == spp->pgs_per_blk) {
                wpp->pg = 0;
                /* move current line to {victim,full} line list */
                if (wpp->curline->vpc == spp->pgs_per_line) {
                    /* all pgs are still valid, move to full line list */
                    assert(wpp->curline->ipc == 0);
                    QTAILQ_INSERT_TAIL(&lm->full_line_list, wpp->curline, entry);
                    lm->full_line_cnt++;
                } else {
                    assert(wpp->curline->vpc >= 0 && wpp->curline->vpc < spp->pgs_per_line);
                    /* there must be some invalid pages in this line */
                    //printf("Coperd,curline,vpc:%d,ipc:%d\n", wpp->curline->vpc, wpp->curline->ipc);
                    my_assert(ssd, wpp->curline->ipc > 0, "error line, id=%d, vpc=%d, ipc=%d, is_RMM_line=%d, is_RMM_page=%d\n", 
                        wpp->curline->id, wpp->curline->vpc, wpp->curline->ipc, wpp->curline->is_RMM_line, is_RMM_page);
                    QTAILQ_INSERT_TAIL(&lm->victim_line_list, wpp->curline, entry);
                    lm->victim_line_cnt++;
                }
                /* current line is used up, pick another empty line */
                check_addr(wpp->blk, spp->blks_per_pl);
                /* TODO: how should we choose the next block for writes */
                wpp->curline = NULL;
                wpp->curline = get_next_free_line(ssd, is_RMM_page);
                if (!wpp->curline) {
                    printf("ssd_advance_write_pointer calling abort\n");
                    abort();
                }
                wpp->blk = wpp->curline->id;
                check_addr(wpp->blk, spp->blks_per_pl);
                /* make sure we are starting from page 0 in the super block */
                assert(wpp->pg == 0);
                assert(wpp->lun == 0);
                assert(wpp->ch == 0);
                /* TODO: assume # of pl_per_lun is 1, fix later */
                assert(wpp->pl == 0);
            }
        }
    }
    //printf("Next,ch:%d,lun:%d,blk:%d,pg:%d\n", wpp->ch, wpp->lun, wpp->blk, wpp->pg);
}

static struct ppa get_new_page(struct ssd *ssd, bool is_RMM_page)
{
    struct write_pointer *wpp = is_RMM_page ? &ssd->wp_RMM : &ssd->wp;
    struct ppa ppa;
    ppa.ppa = 0;
    ppa.g.ch = wpp->ch;
    ppa.g.lun = wpp->lun;
    ppa.g.pg = wpp->pg;
    ppa.g.blk = wpp->blk;
    ppa.g.pl = wpp->pl;
    assert(ppa.g.pl == 0);

    return ppa;
}

static void check_params(struct ssdparams *spp)
{
    /*
     * we are using a general write pointer increment method now, no need to
     * force luns_per_ch and nchs to be power of 2
     */

    //assert(is_power_of_2(spp->luns_per_ch));
    //assert(is_power_of_2(spp->nchs));
}

static void ssd_init_params(struct ssdparams *spp)
{
    spp->secsz = 512;
    spp->secs_per_pg = 8;
    spp->pgs_per_blk = 256;
    spp->blks_per_pl = 320; /*Per SSD: 20GB:320(4)  16GB:256(4+1)  13.3G:213(5+1)  11.4G:182(6+1)  8.8G:142(8+1)*/
    spp->pls_per_lun = 1;
    spp->luns_per_ch = 8;
    spp->nchs = 8;

    spp->pg_rd_lat = 40000;
    spp->pg_wr_lat = 140000;
    spp->blk_er_lat = 3000000;
    spp->ch_xfer_lat = 60000;      // IODA：A Host Device Co-Design for Strong Predictability Contract on Modern Flash Storage (SOSP'21)

    /* calculated values */
    spp->secs_per_blk = spp->secs_per_pg * spp->pgs_per_blk;
    spp->secs_per_pl = spp->secs_per_blk * spp->blks_per_pl;
    spp->secs_per_lun = spp->secs_per_pl * spp->pls_per_lun;
    spp->secs_per_ch = spp->secs_per_lun * spp->luns_per_ch;
    spp->tt_secs = spp->secs_per_ch * spp->nchs;

    spp->pgs_per_pl = spp->pgs_per_blk * spp->blks_per_pl;
    spp->pgs_per_lun = spp->pgs_per_pl * spp->pls_per_lun;
    spp->pgs_per_ch = spp->pgs_per_lun * spp->luns_per_ch;
    spp->tt_pgs = spp->pgs_per_ch * spp->nchs;

    spp->blks_per_lun = spp->blks_per_pl * spp->pls_per_lun;
    spp->blks_per_ch = spp->blks_per_lun * spp->luns_per_ch;
    spp->tt_blks = spp->blks_per_ch * spp->nchs;

    spp->pls_per_ch =  spp->pls_per_lun * spp->luns_per_ch;
    spp->tt_pls = spp->pls_per_ch * spp->nchs;

    spp->tt_luns = spp->luns_per_ch * spp->nchs;

    /* line is special, put it at the end */
    spp->blks_per_line = spp->tt_luns; /* TODO: to fix under multiplanes */
    spp->pgs_per_line = spp->blks_per_line * spp->pgs_per_blk;
    spp->secs_per_line = spp->pgs_per_line * spp->secs_per_pg;
    spp->tt_lines = spp->blks_per_lun; /* TODO: to fix under multiplanes */

    spp->gc_thres_pcent = 0.9;
    spp->gc_thres_lines = (int)((1 - spp->gc_thres_pcent) * spp->tt_lines);
    spp->gc_thres_pcent_high = 0.95;
    spp->gc_thres_lines_high = (int)((1 - spp->gc_thres_pcent_high) * spp->tt_lines);

    printf("spp->pgs_per_line: %d\n", spp->pgs_per_line);
    printf("spp->tt_lines: %d\n", spp->tt_lines);
    printf("spp->tt_blks: %d\n", spp->tt_blks);
    printf("spp->gc_thres_lines: %d\n", spp->gc_thres_lines);

    check_params(spp);
}

static void ssd_init_nand_page(struct nand_page *pg, struct ssdparams *spp)
{
    int i;

    pg->nsecs = spp->secs_per_pg;
    pg->sec = g_malloc0(sizeof(nand_sec_status_t) * pg->nsecs);
    for (i = 0; i < pg->nsecs; i++) {
        pg->sec[i] = SEC_FREE;
        pg->refcount = 0;
    }
    pg->status = PG_FREE;
}

static void ssd_init_nand_blk(struct nand_block *blk, struct ssdparams *spp)
{
    int i;

    blk->npgs = spp->pgs_per_blk;
    blk->pg = g_malloc0(sizeof(struct nand_page) * blk->npgs);
    for (i = 0; i < blk->npgs; i++) {
        ssd_init_nand_page(&blk->pg[i], spp);
    }
    blk->ipc = 0;
    blk->vpc = 0;
    blk->erase_cnt = 0;
    blk->wp = 0;
}

static void ssd_init_nand_plane(struct nand_plane *pl, struct ssdparams *spp)
{
    int i;

    pl->nblks = spp->blks_per_pl;
    pl->blk = g_malloc0(sizeof(struct nand_block) * pl->nblks);
    for (i = 0; i < pl->nblks; i++) {
        ssd_init_nand_blk(&pl->blk[i], spp);
    }
}

static void ssd_init_nand_lun(struct nand_lun *lun, struct ssdparams *spp)
{
    int i;

    lun->npls = spp->pls_per_lun;
    lun->pl = g_malloc0(sizeof(struct nand_plane) * lun->npls);
    for (i = 0; i < lun->npls; i++) {
        ssd_init_nand_plane(&lun->pl[i], spp);
    }
    lun->next_lun_avail_time = 0;
    lun->busy = false;
    //lun->gc_counter = 0;
    //lun->gc_page_counter = 0;
}

static void ssd_init_ch(struct ssd_channel *ch, struct ssdparams *spp)
{
    int i;

    ch->nluns = spp->luns_per_ch;
    ch->lun = g_malloc0(sizeof(struct nand_lun) * ch->nluns);
    for (i = 0; i < ch->nluns; i++) {
        ssd_init_nand_lun(&ch->lun[i], spp);
    }
    ch->next_ch_avail_time = 0;
    ch->busy = 0;
}

static void ssd_init_maptbl(struct ssd *ssd)
{
    int i;
    struct ssdparams *spp = &ssd->sp;

    ssd->maptbl = g_malloc0(sizeof(struct ppa) * spp->tt_pgs);
    for (i = 0; i < spp->tt_pgs; i++) {
        ssd->maptbl[i].ppa = UNMAPPED_PPA;
    }
}

static void ssd_init_rmap(struct ssd *ssd)
{
    int i;
    struct ssdparams *spp = &ssd->sp;
    ssd->rmap = g_malloc0(sizeof(struct rmap_elem) * spp->tt_pgs);
    for (i = 0; i < spp->tt_pgs; i++) {
        ssd->rmap[i].lpn = INVALID_LPN;
        ssd->rmap[i].RMM_page_p = NULL;
    }
}

void ssd_init(struct ssd *ssd)
{
    int i;
    struct ssdparams *spp = &ssd->sp;

    assert(ssd);

    //pages_read = 0;
    /* assign ssd id for GC synchronization */
    ssd->id = ssd_count++;
    printf("GCSYNC SSD initialized with id %d\n", ssd->id);
	ssd->next_ssd_avail_time = 0;
    ssd->last_print_time_s = 0;
    ssd->test_begin = true;

    ssd_init_params(spp);

    /* initialize ssd internal layout architecture */
    ssd->ch = g_malloc0(sizeof(struct ssd_channel) * spp->nchs);
    for (i = 0; i < spp->nchs; i++) {
        ssd_init_ch(&ssd->ch[i], spp);
    }

    /* initialize maptbl */
    ssd_init_maptbl(ssd);

    /* initialize rmap */
    ssd_init_rmap(ssd);

    /* initialize all the lines */
    ssd_init_lines(ssd);

    /* initialize write pointer, this is how we allocate new pages for writes */
    ssd_init_write_pointer(ssd);

    /* wen:added  Remap-SSD */
    ssd_init_segments(ssd);
    ssd_init_write_RMM_pointer(ssd);
    ssd_init_GC_migration_mappins(ssd);

    qemu_thread_create(&ssd->ftl_thread, "ftl_thread", ftl_thread, ssd,
            QEMU_THREAD_JOINABLE);
}

inline bool valid_ppa(struct ssd *ssd, struct ppa *ppa)
{
    struct ssdparams *spp = &ssd->sp;
    int ch = ppa->g.ch;
    int lun = ppa->g.lun;
    int pl = ppa->g.pl;
    int blk = ppa->g.blk;
    int pg = ppa->g.pg;
    int sec = ppa->g.sec;

    if (ch >= 0 && ch < spp->nchs && lun >= 0 && lun < spp->luns_per_ch &&
            pl >= 0 && pl < spp->pls_per_lun && blk >= 0 &&
            blk < spp->blks_per_pl && pg >= 0 && pg < spp->pgs_per_blk &&
            sec >= 0 && sec < spp->secs_per_pg)
        return true;

    return false;
}

static inline bool valid_lpn(struct ssd *ssd, uint64_t lpn)
{
    return (lpn < ssd->sp.tt_pgs || lpn == RMM_PAGE);
}

inline bool mapped_ppa(struct ppa *ppa)
{
    return !(ppa->ppa == UNMAPPED_PPA);
}

static inline struct ssd_channel *get_ch(struct ssd *ssd, struct ppa *ppa)
{
    my_assert(ssd, ppa->g.ch < ssd->sp.nchs, "Error: ppa->g.ch >= ssd->sp.nchs");
    return &(ssd->ch[ppa->g.ch]);
}

static inline struct nand_lun *get_lun(struct ssd *ssd, struct ppa *ppa)
{
    my_assert(ssd, ppa->g.lun < ssd->sp.luns_per_ch, "Error: ppa->g.lun >= ssd->sp.luns_per_ch");
    struct ssd_channel *ch = get_ch(ssd, ppa);
    return &(ch->lun[ppa->g.lun]);
}

static inline struct nand_plane *get_pl(struct ssd *ssd, struct ppa *ppa)
{
    my_assert(ssd, ppa->g.pl < ssd->sp.pls_per_lun, "Error: ppa->g.pl >= ssd->sp.pls_per_lun");
    struct nand_lun *lun = get_lun(ssd, ppa);
    return &(lun->pl[ppa->g.pl]);
}

static inline struct nand_block *get_blk(struct ssd *ssd, struct ppa *ppa)
{
    my_assert(ssd, ppa->g.blk < ssd->sp.blks_per_pl, "Error: ppa->g.blk >= ssd->sp.blks_per_pl");
    struct nand_plane *pl = get_pl(ssd, ppa);
    return &(pl->blk[ppa->g.blk]);
}

static inline struct line *get_line(struct ssd *ssd, struct ppa *ppa)
{
    my_assert(ssd, ppa->g.blk < ssd->lm.tt_lines, "Error: ppa->g.blk >= ssd->lm.tt_lines");
    return &(ssd->lm.lines[ppa->g.blk]);
}

static inline struct nand_page *get_pg(struct ssd *ssd, struct ppa *ppa)
{
    my_assert(ssd, ppa->g.pg < ssd->sp.pgs_per_blk, "Error: ppa->g.pg >= ssd->sp.pgs_per_blk");
    struct nand_block *blk = get_blk(ssd, ppa);
    return &(blk->pg[ppa->g.pg]);
}

static uint64_t ssd_advance_status(struct ssd *ssd, struct ppa *ppa,
        struct nand_cmd *ncmd)
{
    int c = ncmd->cmd;
    uint64_t nand_stime;
    //uint64_t chnl_stime;
    struct ssdparams *spp = &ssd->sp;
    //struct ssd_channel *ch = get_ch(ssd, ppa);
    struct nand_lun *lun = get_lun(ssd, ppa);
    uint64_t cmd_stime = (ncmd->stime == 0) ? \
        qemu_clock_get_ns(QEMU_CLOCK_REALTIME) : ncmd->stime;
	// Why were we using current time? That doesn't seem to make sense as when to "start" a GC operation ever
    //uint64_t cmd_stime = (ncmd->stime == 0) ? \
        lun->next_lun_avail_time : ncmd->stime;
    uint64_t lat = 0;

    switch (c) {
    case NAND_READ:
        /* read: perform NAND cmd first */
        nand_stime = (lun->next_lun_avail_time < cmd_stime) ? cmd_stime : \
                     lun->next_lun_avail_time;
        lun->next_lun_avail_time = nand_stime + spp->pg_rd_lat + spp->ch_xfer_lat;
        lat = lun->next_lun_avail_time - cmd_stime;
        break;

    case NAND_WRITE:
        /* write: transfer data through channel first */
        nand_stime = (lun->next_lun_avail_time < cmd_stime) ? cmd_stime : \
                     lun->next_lun_avail_time;
        lun->next_lun_avail_time = nand_stime + spp->pg_wr_lat + spp->ch_xfer_lat;
        lat = lun->next_lun_avail_time - cmd_stime;
        break;

    case NAND_ERASE:
        /* erase: only need to advance NAND status */
        nand_stime = (lun->next_lun_avail_time < cmd_stime) ? cmd_stime : \
                     lun->next_lun_avail_time;
        lun->next_lun_avail_time = nand_stime + spp->blk_er_lat;

        lat = lun->next_lun_avail_time - cmd_stime;
        break;

    default:
        printf("Unsupported NAND command: 0x%x\n", c);
    }

	if (lun->next_lun_avail_time > ssd->next_ssd_avail_time) {
		ssd->next_ssd_avail_time = lun->next_lun_avail_time;
	}

    printf_info(ssd, false);

    return lat;
}

static uint64_t nvram_advance_status(struct ssd *ssd, struct nand_cmd *ncmd, int count)
{
    int c = ncmd->cmd;
    uint64_t nvram_stime;
    struct ssdparams *spp = &ssd->sp;
    uint64_t cmd_stime = (ncmd->stime == 0) ? \
        qemu_clock_get_ns(QEMU_CLOCK_REALTIME) : ncmd->stime;
    uint64_t lat = 0;

    switch (c) {
    case NVRAM_READ:
        nvram_stime = (ssd->next_NVRAM_avail_time < cmd_stime) ? cmd_stime : \
                     ssd->next_NVRAM_avail_time;
        ssd->next_NVRAM_avail_time = nvram_stime + nvram_rd_lat_per_RMM * count;
        lat = ssd->next_NVRAM_avail_time - cmd_stime;
        break;

    case NVRAM_WRITE:
        nvram_stime = (ssd->next_NVRAM_avail_time < cmd_stime) ? cmd_stime : \
                     ssd->next_NVRAM_avail_time;
        ssd->next_NVRAM_avail_time = nvram_stime + nvram_wr_lat_per_RMM * count;
        lat = ssd->next_NVRAM_avail_time - cmd_stime;
        break;

    default:
        printf("Unsupported NVRAM command: 0x%x\n", c);
    }

	if (ssd->next_NVRAM_avail_time > ssd->next_ssd_avail_time) {
		ssd->next_ssd_avail_time = ssd->next_NVRAM_avail_time;
	}

    printf_info(ssd, false);

    return lat;
}

/* update SSD status about one page from PG_VALID -> PG_VALID */
static void mark_page_invalid(struct ssd *ssd, struct ppa *ppa)
{
    struct line_mgmt *lm = &ssd->lm;
    struct ssdparams *spp = &ssd->sp;
    struct nand_block *blk = NULL;
    struct nand_page *pg = NULL;
    bool was_full_line = false;
    struct line *line;

    /* update corresponding page status */
    pg = get_pg(ssd, ppa);
    assert(pg->status == PG_VALID);
    pg->status = PG_INVALID;

    /* update corresponding block status */
    blk = get_blk(ssd, ppa);
    assert(blk->ipc >= 0 && blk->ipc < spp->pgs_per_blk);
    blk->ipc++;
    assert(blk->vpc > 0 && blk->vpc <= spp->pgs_per_blk);
    blk->vpc--;

    /* update corresponding line status */
    line = get_line(ssd, ppa);
    assert(line->ipc >= 0 && line->ipc < spp->pgs_per_line);
    if (line->vpc == spp->pgs_per_line) {
        assert(line->ipc == 0);
        was_full_line = true;
    }
    line->ipc++;
    assert(line->vpc > 0 && line->vpc <= spp->pgs_per_line);
    line->vpc--;
    if (was_full_line) {
        /* move line: "full" -> "victim" */
        QTAILQ_REMOVE(&lm->full_line_list, line, entry);
        lm->full_line_cnt--;
        QTAILQ_INSERT_TAIL(&lm->victim_line_list, line, entry);
        lm->victim_line_cnt++;
    }
}

/* update SSD status about one page from PG_FREE -> PG_VALID */
static void mark_page_valid(struct ssd *ssd, struct ppa *ppa)
{
    struct ssdparams *spp = &ssd->sp;
    struct nand_block *blk = NULL;
    struct nand_page *pg = NULL;
    struct line *line;

    /* update page status */
    pg = get_pg(ssd, ppa);
    my_assert(ssd, pg->status == PG_FREE, "ppa(ch:%d)(lun:%d)(pl:%d)(blk:%d)(pg:%d)(sec:%d)(ppa:%d) status(%d)\n",
        ppa->g.ch, ppa->g.lun, ppa->g.pl, ppa->g.blk, ppa->g.pg, ppa->g.sec, ppa->ppa, pg->status);
    pg->status = PG_VALID;

    /* update corresponding block status */
    blk = get_blk(ssd, ppa);
    assert(blk->vpc >= 0 && blk->vpc < spp->pgs_per_blk);
    blk->vpc++;

    /* update corresponding line status */
    line = get_line(ssd, ppa);
    assert(line->vpc >= 0 && line->vpc < spp->pgs_per_line);
    line->vpc++;
}

/* only for erase, reset one block to free state */
static void mark_block_free(struct ssd *ssd, struct ppa *ppa)
{
    struct ssdparams *spp = &ssd->sp;
    struct nand_block *blk = get_blk(ssd, ppa);
    struct nand_page *pg = NULL;
    int i;

    for (i = 0; i < spp->pgs_per_blk; i++) {
        /* reset page status */
        pg = &blk->pg[i];
        assert(pg->nsecs == spp->secs_per_pg);
        my_assert(ssd, pg->status == PG_INVALID, "Error: pg->status != PG_INVALID");
        pg->status = PG_FREE;
        assert(pg->refcount == 0);
    }

    /* reset block status */
    assert(blk->npgs == spp->pgs_per_blk);
    blk->ipc = 0;
    blk->vpc = 0;
    blk->erase_cnt++;
}

/* assume the read data will staged in DRAM and then flushed back to NAND */
static void gc_read_page(struct ssd *ssd, struct ppa *ppa)
{
    ssd->tt_GC_IOs[TOTAL][NAND_READ]++;
    ssd->tt_GC_IOs[LAST_SECOND][NAND_READ]++;
    struct nand_cmd gcr;
    gcr.cmd = NAND_READ;
    gcr.stime = 0;
    ssd_advance_status(ssd, ppa, &gcr);
}

/* move valid page data (already in DRAM) from victim line to a new page */
static uint64_t gc_write_page(struct ssd *ssd, struct ppa *old_ppa, bool is_RMM_page)
{
    struct ppa new_ppa;
    struct ssd_channel *new_ch;
    struct nand_lun *new_lun;
    struct rmap_elem elem = get_rmap_ent(ssd, old_ppa);  //TODO: may not rmap in M-Table (lpn == INVALID_LPN), may multiple rmaps in R-Table
    /* first read out current mapping info */
    //set_rmap(ssd, lpn, new_ppa);

    if(elem.lpn == INVALID_LPN) {
        //this flash page is only referenced by remap-lpns whose rmap info is stored in RMMs
        //thus, reference update of these remap-lpns is completed in segment_migration()
        ssd->GC_migration_mappings[ppa_to_OffsetInLine(ssd, old_ppa)].ppa = DELAY_WRITE;    //usded for segment_migration()
        ssd->wait_migrate_RMMs += get_pg(ssd, old_ppa)->refcount;
        return 0;
    }
    else {
        new_ppa = get_new_page(ssd, is_RMM_page);

        if(elem.lpn == RMM_PAGE) {
            my_assert(ssd, is_RMM_page, "Error: elem.lpn == RMM_PAGE but it is not a RMM_PAGE");
            my_assert(ssd, elem.RMM_page_p != NULL, "Error: elem.RMM_page_p is NULL");
            my_assert(ssd, elem.RMM_page_p->ppa.ppa == old_ppa->ppa, "Error: elem.RMM_page_p->ppa.ppa != old_ppa->ppa");
            delete_reference(ssd, false, old_ppa, elem);
            my_assert(ssd, get_pg(ssd, old_ppa)->refcount == 0, "Error: get_pg(ssd, old_ppa)->refcount != 0");
            elem.RMM_page_p->ppa = new_ppa;
            add_reference(ssd, false, &new_ppa, true, elem);
        }
        //may not rmap in M-Table (elem.lpn == INVALID_LPN), but there are multiple rmaps in R-Table whose updates of reference are processed in RMM_migration function
        //elem.lpn != INVALID_LPN
        else {
            my_assert(ssd, elem.RMM_page_p == NULL, "Error: elem.RMM_page_p is not NULL");
            delete_reference(ssd, false, old_ppa, elem);
            ssd->wait_migrate_RMMs += get_pg(ssd, old_ppa)->refcount;
            ssd->GC_migration_mappings[ppa_to_OffsetInLine(ssd, old_ppa)] = new_ppa;
            add_reference(ssd, false, &new_ppa, true, elem);    //lpn recored in rmap-ent(OOB) must be a remote lpn
        }
        //otherwise, this flash page is only referenced by remap-lpns whose rmap info is stored in RMMs
        //thus, reference update of these remap-lpns is completed in segment_migration()

        /* need to advance the write pointer here */
        ssd_advance_write_pointer(ssd, is_RMM_page);

        ssd->tt_GC_IOs[TOTAL][NAND_WRITE]++;
        ssd->tt_GC_IOs[LAST_SECOND][NAND_WRITE]++;
        struct nand_cmd gcw;
        gcw.cmd = NAND_WRITE;
        gcw.stime = 0;
        ssd_advance_status(ssd, &new_ppa, &gcw);
        return 0;
    }
}

/* TODO: now O(n) list traversing, optimize it later */
static struct line *select_victim_line(struct ssd *ssd, bool force)
{
    struct line_mgmt *lm = &ssd->lm;
    struct line *line, *victim_line = NULL;
    int max_ipc = 0;
    //int cnt = 0;

    if (QTAILQ_EMPTY(&lm->victim_line_list)) {
        //printf("QTAILQ_EMPTY(&lm->victim_line_list)\n");
        return NULL;
    }

    QTAILQ_FOREACH(line, &lm->victim_line_list, entry) {
        //printf("Coperd,%s,victim_line_list[%d],ipc=%d,vpc=%d\n", __func__, ++cnt, line->ipc, line->vpc);
        if (line->ipc > max_ipc) {
            victim_line = line;
            max_ipc = line->ipc;
        }
    }

    my_assert(ssd, max_ipc > 0, "Error: there is no invalid page in all flash blocks");
    if(!victim_line)
        return NULL;

    if (!force && victim_line->ipc < ssd->sp.pgs_per_line / 4) {
        //printf("Coperd,select a victim line: ipc=%d (< 1/8), pgs_per_line=%d\n", victim_line->ipc, ssd->sp.pgs_per_line);
        return NULL;
    }

    QTAILQ_REMOVE(&lm->victim_line_list, victim_line, entry);
    lm->victim_line_cnt--;
    //printf("Coperd,%s,victim_line_list,chooose-victim-block,id=%d,ipc=%d,vpc=%d\n", __func__, victim_line->id, victim_line->ipc, victim_line->vpc);

    /* victim_line is a danggling node now */
    return victim_line;
}

/* here ppa identifies the block we want to clean 
    Returns the number of valid pages we needed to copy within the block*/
static int clean_one_block(struct ssd *ssd, struct ppa *ppa, bool is_RMM_block)
{
    struct ssdparams *spp = &ssd->sp;
    struct nand_block *blk = get_blk(ssd, ppa);
    struct nand_page *pg_iter = NULL;
    int cnt = 0;
    int pg;
    int old_blk_vpc = blk->vpc;

    for (pg = 0; pg < spp->pgs_per_blk; pg++) {
        ppa->g.pg = pg;
        pg_iter = get_pg(ssd, ppa);
        /* there shouldn't be any free page in victim blocks */
        assert(pg_iter->status != PG_FREE);
        if (pg_iter->status == PG_VALID) {
            my_assert(ssd, pg_iter->refcount > 0, "error, refcount of a valid page (ppa=%lu) is %d\n", ppa->ppa, pg_iter->refcount);
            gc_read_page(ssd, ppa);
            /* delay the maptbl update until "write" happens */
            gc_write_page(ssd, ppa, is_RMM_block);
            cnt++;
        }
    }

    my_assert(ssd, old_blk_vpc == cnt, "valid_pages=%d, migrate_pages=%d\n", old_blk_vpc, cnt);
    /* do we do "erase" here? */
    ssd->tt_GC_IOs[TOTAL][NAND_ERASE]++;
    ssd->tt_GC_IOs[LAST_SECOND][NAND_ERASE]++;
    struct nand_cmd gce;
    gce.cmd = NAND_ERASE;
    gce.stime = 0;
    ssd_advance_status(ssd, ppa, &gce);
    return cnt;
}

static void mark_line_free(struct ssd *ssd, struct ppa *ppa)
{
    struct line_mgmt *lm = &ssd->lm;
    struct line *line = get_line(ssd, ppa);
    line->ipc = 0;
    line->vpc = 0;
    
    my_assert(ssd, line->segment_count_inNVRAM == 0, 
        "error in mark_line_free, line->segment_count_inNVRAM(%d) != 0\n", line->segment_count_inNVRAM);
    line->next_segment_id = 0;
    line->is_RMM_line = false;
    /* move this line to free line list */
    QTAILQ_INSERT_TAIL(&lm->free_line_list, line, entry);
    lm->free_line_cnt++;
    //printf("Coperd,%s,one more free line,free_line_cnt=%d\n", __func__, lm->free_line_cnt);
}

static int do_gc(struct ssd *ssd, bool force, NvmeRequest *req)
{
    struct line *victim_line = NULL;
    struct ssdparams *spp = &ssd->sp;
    struct ssd_channel *chp;
    struct nand_lun *lunp;
    struct ppa ppa;
    int ch, lun;

    victim_line = select_victim_line(ssd, force);
    if (!victim_line) {
        return -1;
    }

    ppa.ppa = 0;
    ppa.g.blk = victim_line->id;
    /* copy back valid data */
    for (ch = 0; ch < spp->nchs; ch++) {
        for (lun = 0; lun < spp->luns_per_ch; lun++) {
            ppa.g.ch = ch;
            ppa.g.lun = lun;
            ppa.g.pl = 0;
            chp = get_ch(ssd, &ppa);
            lunp = get_lun(ssd, &ppa);
            clean_one_block(ssd, &ppa, victim_line->is_RMM_line);
        }
    }

    /* wen:added  Remap-SSD */
    segmentgroup_migration(ssd, victim_line);
    ssd_init_GC_migration_mappins(ssd);

    /* update line status */
    for (ch = 0; ch < spp->nchs; ch++) {
        for (lun = 0; lun < spp->luns_per_ch; lun++) {
            ppa.g.ch = ch;
            ppa.g.lun = lun;
            ppa.g.pl = 0;
            mark_block_free(ssd, &ppa);
        }
    }
    mark_line_free(ssd, &ppa);

    return 0;
}

static void *ftl_thread(void *arg)
{
    struct ssd *ssd = (struct ssd *)arg;
    NvmeRequest *req = NULL;
    uint64_t lat = 0;
    int rc;
    uint64_t cpu_cycle_begin, cpu_cycle_end;
    //unsigned int zero_in_q = 0, one_in_q = 0, two_in_q = 0, three_in_q = 0, four_or_more_in_q = 0;
    //unsigned int total_serviced = 0;

    while (!*(ssd->dataplane_started_ptr)) {
        usleep(100000);
    }

    sprintf(ssd->info_file_name, "./SSDInfo_%d.log", ssd->id);
    init_file(ssd->fp_info, ssd->info_file_name);
    open_file(ssd->fp_info, ssd->info_file_name, "a");
    // sprintf(ssd->latency_file_name, "./SSDLatency_%d.log", ssd->id);
    // init_file(ssd->fp_latency, ssd->latency_file_name);
    // open_file(ssd->fp_latency, ssd->latency_file_name, "a");

    while (1) {
        if (!ssd->to_ftl || !femu_ring_count(ssd->to_ftl))
            continue;

        rc = femu_ring_dequeue(ssd->to_ftl, (void *)&req, 1);
        if (rc != 1) {
            printf("FEMU: FTL to_ftl dequeue failed\n");
        }
        assert(req);

        cpu_cycle_begin = read_tsc();

        switch (req->opcode) {
            case NVME_CMD_WRITE:
                // my_log(ssd->fp_latency, "receive IO(Write):%ld,\t", req->slba);
                lat = ssd_write(ssd, req);
                break;
            case NVME_CMD_READ:
                // my_log(ssd->fp_latency, "receive IO(Read):%ld,\t", req->slba);
                lat = ssd_read(ssd, req);
                if (lat > 1e9) {
                    printf("FEMU: Read latency is > 1s, what's going on!\n");
                }
                break;
            //wen:added
            case NVME_CMD_DEDUP_WRITE:
                // my_log(ssd->fp_latency, "receive IO(Remap):%ld,\t", req->slba);
                lat = ssd_dedup_write(ssd, req);
                break;
            case NVME_CMD_REMOTE_READ:
                // my_log(ssd->fp_latency, "receive IO(RemoteRead):%ld,\t", req->slba);
                lat = ssd_remote_read(ssd, req);
                break;
            case NVME_CMD_REMOTE_PARITY_WRITE:
                lat = ssd_remote_parity_write(ssd, req);
                break;
            case NVME_CMD_DSM:
                // my_log(ssd->fp_latency, "receive IO(Trim):%ld,\t", req->slba);
                lat = ssd_trim(ssd, req);
                break;
            default:
                printf("FEMU: FTL received unkown request type, ERROR\n");
        }

        // my_log(ssd->fp_latency, "finish IO:%ld\n", req->slba);

        req->reqlat = lat;
        req->expire_time += lat;

        cpu_cycle_end = read_tsc();
        ssd->cpu_cycle_tt += (cpu_cycle_end-cpu_cycle_begin);
        
        rc = femu_ring_enqueue(ssd->to_poller, (void *)&req, 1);
        if (rc != 1) {
            printf("FEMU: FTL to_poller enqueue failed\n");
        }

        /* clean one line if needed (in the background) */
        if (should_gc(ssd)) {
			do_gc(ssd, false, req);
        }
    }
}

/* accept NVMe cmd as input, in order to support more command types in future */
uint64_t ssd_read(struct ssd *ssd, NvmeRequest *req)
{
    /* TODO: reads need to go through caching layer first */
    /* ... */


    /* on cache miss, read from NAND */
    struct ssdparams *spp = &ssd->sp;
    uint64_t lba = req->slba; /* sector addr */
    int nsecs = req->nlb;
    struct ppa ppa;
    uint64_t start_lpn = lba / spp->secs_per_pg;
    uint64_t end_lpn = (lba + nsecs) / spp->secs_per_pg;
    uint64_t lpn;
    uint64_t sublat, maxlat = 0;

    if (end_lpn >= spp->tt_pgs) {
        printf("RD-ERRRRRRRRRR,start_lpn=%"PRIu64",end_lpn=%"PRIu64",tt_pgs=%d\n", start_lpn, end_lpn, ssd->sp.tt_pgs);
    }

    /* normal IO read path */
    for (lpn = start_lpn; lpn <= end_lpn; lpn++) {
        ppa = get_maptbl_ent(ssd, lpn, false);
        if (!mapped_ppa(&ppa) || !valid_ppa(ssd, &ppa)) {
            continue;
        }

        ssd->tt_IOs[TOTAL][NAND_READ][lpn >= ssd->metadata_offset ? USER_IO : METADATA_IO]++;
        ssd->tt_IOs[LAST_SECOND][NAND_READ][lpn >= ssd->metadata_offset ? USER_IO : METADATA_IO]++;
        struct nand_cmd srd;
        srd.cmd = NAND_READ;
        srd.stime = req->stime;
        sublat = ssd_advance_status(ssd, &ppa, &srd);
        maxlat = (sublat > maxlat) ? sublat : maxlat;
    }
    return maxlat;
}

uint64_t ssd_write(struct ssd *ssd, NvmeRequest *req)
{
    uint64_t lba = req->slba;
    struct ssdparams *spp = &ssd->sp;
    int len = req->nlb;
    uint64_t start_lpn = lba / spp->secs_per_pg;
    uint64_t end_lpn = (lba + len - 1) / spp->secs_per_pg;
    struct ppa ppa;
    uint64_t lpn;
    uint64_t curlat = 0, maxlat = 0;
    int r;
    /* TODO: writes need to go to cache first */
    /* ... */

    if (end_lpn >= spp->tt_pgs) {
        printf("ERRRRRRRRRR,start_lpn=%"PRIu64",end_lpn=%"PRIu64",tt_pgs=%d\n", start_lpn, end_lpn, ssd->sp.tt_pgs);
    }
    //assert(end_lpn < spp->tt_pgs);
    //printf("Coperd,%s,end_lpn=%"PRIu64" (%d),len=%d\n", __func__, end_lpn, spp->tt_pgs, len);

    while (should_gc_high(ssd)) {
        /* perform GC here until !should_gc(ssd) */
        //printf("FEMU: FTL doing blocking GC\n");
        r = do_gc(ssd, true, NULL);
        if (r == -1)
            break;
    }

    /* on cache eviction, write to NAND page */

    // are we doing fresh writes ? maptbl[lpn] == FREE, pick a new page
    for (lpn = start_lpn; lpn <= end_lpn; lpn++) {
        struct rmap_elem elem;
        elem.lpn = lpn;
        elem.RMM_page_p = NULL;

        ppa = get_maptbl_ent(ssd, lpn, false);
        if (mapped_ppa(&ppa)) {
            /* overwrite */
            /* update old page information first */
            //printf("Coperd,before-overwrite,line[%d],ipc=%d,vpc=%d\n", ppa.g.blk, get_line(ssd, &ppa)->ipc, get_line(ssd, &ppa)->vpc);
            delete_reference(ssd, false, &ppa, elem);
        }

        /* new write */
        /* find a new page */
        ppa = get_new_page(ssd, false);

        add_reference(ssd, false, &ppa, true, elem);

        /* need to advance the write pointer here */
        ssd_advance_write_pointer(ssd, false);

        ssd->tt_IOs[TOTAL][NAND_WRITE][lpn >= ssd->metadata_offset ? USER_IO : METADATA_IO]++;
        ssd->tt_IOs[LAST_SECOND][NAND_WRITE][lpn >= ssd->metadata_offset ? USER_IO : METADATA_IO]++;
        struct nand_cmd swr;
        swr.cmd = NAND_WRITE;
        swr.stime = req->stime;
        /* get latency statistics */
        curlat = ssd_advance_status(ssd, &ppa, &swr);
        maxlat = (curlat > maxlat) ? curlat : maxlat;
    }

    return maxlat;
}

uint64_t ssd_remote_read(struct ssd *ssd, NvmeRequest *req)
{
    struct ssdparams *spp = &ssd->sp;

    ssd->tt_IOs[TOTAL][NAND_READ][USER_IO]++;
    ssd->tt_IOs[LAST_SECOND][NAND_READ][USER_IO]++;
    
    struct ppa ppa = get_maptbl_ent(ssd, req->remote_entry_offset, true);
    if(!mapped_ppa(&ppa) || !valid_ppa(ssd, &ppa))  return 0;

    struct nand_cmd srd;
    uint64_t lat = 0;
    srd.cmd = NAND_READ;
    srd.stime = req->stime;
    lat = ssd_advance_status(ssd, &ppa, &srd);
    return lat;
}

uint64_t ssd_dedup_write(struct ssd *ssd, NvmeRequest *req)
{
    // ssd->tt_IOs[TOTAL][NAND_WRITE][USER_IO]++;
    // ssd->tt_IOs[LAST_SECOND][NAND_WRITE][USER_IO]++;
    ssd->tt_remaps[TOTAL]++;
    ssd->tt_remaps[LAST_SECOND]++;

    struct ssdparams *spp = &ssd->sp;
    uint64_t local_lpn = req->slba / spp->secs_per_pg;    //lpn = tgt_lpn / N, N is the count of SSDs
    uint64_t src_lpn = req->src_slba / spp->secs_per_pg;

    struct ppa ppa = get_maptbl_ent(ssd, src_lpn, false);
    if(!mapped_ppa(&ppa))  return 0;

    //TODO: NVMe may execute request out of order, such as excuting Remap before Write.
    // if(ppa.ppa == UNMAPPED_PPA)

    struct rmap_elem elem;
    elem.lpn = req->is_remote_slba ? req->remote_entry_offset : local_lpn;
    elem.RMM_page_p = NULL;

    struct ppa old_ppa = get_maptbl_ent(ssd, elem.lpn, req->is_remote_slba);
    if (mapped_ppa(&old_ppa)) {
        delete_reference(ssd, req->is_remote_slba, &old_ppa, elem);
    }

    my_assert(ssd, mapped_ppa(&ppa) && valid_ppa(ssd, &ppa),
        "ssd(%d):%s, ppa:%d, local_lpn:%x, src_lpn:%x, if_mapped:%d, if_valid:%d, if_trim:%d, is_remote_slba:%d, entry_offset:%d, global_slba:%x\n",
        ssd->id, ssd->ssdname, ppa.ppa, local_lpn, src_lpn, mapped_ppa(&ppa), valid_ppa(ssd, &ppa), mapped_ppa(&old_ppa), req->is_remote_slba, req->remote_entry_offset, req->global_slba);
    add_reference(ssd, req->is_remote_slba, &ppa, false, elem);

    struct RMM* RMM = NULL;
    RMM = get_RMM(ssd, get_line(ssd, &ppa)); 
    my_assert(ssd, RMM != NULL, "Error: RMM is null");
    RMM->torn_bit_0 = 1;
    RMM->offset = ppa_to_OffsetInLine(ssd, &ppa);
    RMM->number = ssd->cur_write_number++;
    RMM->torn_bit_1 = 1;
    RMM->if_remote_lpn = req->is_remote_slba;
    // RMM->target_LPN = req->is_remote_slba ? req->global_slba / spp->secs_per_pg : local_lpn;
    RMM->target_LPN = elem.lpn;
    my_assert(ssd, is_valid_RMM(ssd, RMM, get_line(ssd, &ppa)->id), 
        "Remap:\tis_remote(%d), local_lpn(%x), src_lpn(%x), metadata_offset(%x), ssd_count(%d), global_slba:%x\n \
        RMM:\toffset(%x)(%x), if_remote(%d)(%d), target_LPN(%d)(%d), segment_line_id(%d)\n \
        assert:\tppa(%x), mapped_ppa(%x), recovered_ppa(%x)\n",
        req->is_remote_slba, local_lpn, src_lpn, ssd->metadata_offset, ssd_count, req->global_slba,
        RMM->offset, ppa_to_OffsetInLine(ssd, &ppa), RMM->if_remote_lpn, req->is_remote_slba, RMM->target_LPN, elem.lpn, get_line(ssd, &ppa)->id,
        ppa.ppa, get_maptbl_ent(ssd, RMM->target_LPN, RMM->if_remote_lpn).ppa, OffsetInLine_to_ppa(ssd, RMM->offset, get_line(ssd, &ppa)->id).ppa);

    struct nand_cmd nvrd;
    uint64_t lat = 0;
    nvrd.cmd = NVRAM_WRITE;
    nvrd.stime = req->stime;
    lat = nvram_advance_status(ssd, &nvrd, 1);
    return lat;
}

uint64_t ssd_remote_parity_write(struct ssd *ssd, NvmeRequest *req)
{
    struct ssdparams *spp = &ssd->sp;
    struct ppa ppa;
    uint64_t lpn = (uint64_t)spp->tt_pgs + (uint64_t)req->remote_entry_offset;
    my_assert(ssd, req->remote_entry_offset < ssd->sp.tt_remote_pgs, 
        "error: req->remote_entry_offset(%d) < ssd->sp.tt_remote_pgs(%d)", req->remote_entry_offset, ssd->sp.tt_remote_pgs);
    uint64_t curlat = 0, maxlat = 0;
    int r;
    bool is_remote = false;     //TODO
    bool is_rmap = true;        //TODO

    while (should_gc_high(ssd)) {
        /* perform GC here until !should_gc(ssd) */
        //printf("FEMU: FTL doing blocking GC\n");
        r = do_gc(ssd, true, NULL);
        if (r == -1)
            break;
    }

    struct rmap_elem elem;
    elem.lpn = lpn;
    elem.RMM_page_p = NULL;

    ppa = get_maptbl_ent(ssd, lpn, is_remote);
    if (mapped_ppa(&ppa)) {
        delete_reference(ssd, is_remote, &ppa, elem);
    }

    ppa = get_new_page(ssd, false);
    add_reference(ssd, is_remote, &ppa, is_rmap, elem);

    ssd_advance_write_pointer(ssd, false);

    ssd->tt_IOs[TOTAL][NAND_WRITE][USER_IO]++;
    ssd->tt_IOs[LAST_SECOND][NAND_WRITE][USER_IO]++;
    struct nand_cmd swr;
    swr.cmd = NAND_WRITE;
    swr.stime = req->stime;
    /* get latency statistics */
    curlat = ssd_advance_status(ssd, &ppa, &swr);
    maxlat = (curlat > maxlat) ? curlat : maxlat;

    return maxlat;
}

uint64_t ssd_trim(struct ssd *ssd, NvmeRequest *req)
{
    if(req->is_special_cmd) {
        if(req->special_value == -1) {
            ssd->test_begin = true;
            memset(ssd->tt_IOs, 0, sizeof(int)*2*2*2);
            memset(ssd->tt_GC_IOs, 0, sizeof(int)*2*3);
            memset(ssd->tt_RMM_IOs, 0, sizeof(int)*2*2);
            memset(ssd->tt_remaps, 0, sizeof(int)*2);
            memset(ssd->tt_trims, 0, sizeof(int)*2);
            // ssd->tt_IOs[TOTAL][NAND_READ][USER_IO] = ssd->tt_IOs[TOTAL][NAND_READ][METADATA_IO] = ssd->tt_IOs[TOTAL][NAND_WRITE][USER_IO] = ssd->tt_IOs[TOTAL][NAND_WRITE][METADATA_IO] = ssd->tt_IOs[LAST_SECOND][NAND_READ][USER_IO] = ssd->tt_IOs[LAST_SECOND][NAND_READ][METADATA_IO] = ssd->tt_IOs[LAST_SECOND][NAND_WRITE][USER_IO] = ssd->tt_IOs[LAST_SECOND][NAND_WRITE][METADATA_IO] = 0;
            // ssd->tt_GC_IOs[TOTAL][NAND_READ] = ssd->tt_GC_IOs[TOTAL][NAND_WRITE] = ssd->tt_GC_IOs[TOTAL][NAND_ERASE] = ssd->tt_GC_IOs[LAST_SECOND][NAND_READ] = ssd->tt_GC_IOs[LAST_SECOND][NAND_WRITE] = ssd->tt_GC_IOs[LAST_SECOND][NAND_ERASE] = 0;
            // ssd->tt_RMM_IOs[TOTAL][NAND_READ] = ssd->tt_RMM_IOs[TOTAL][NAND_WRITE] = ssd->tt_RMM_IOs[LAST_SECOND][NAND_READ] = ssd->tt_RMM_IOs[LAST_SECOND][NAND_WRITE] = 0;
            // ssd->tt_remaps[TOTAL] = ssd->tt_remaps[LAST_SECOND] = 0;
            // ssd->tt_trims[TOTAL] = ssd->tt_trims[LAST_SECOND] = 0;
            ssd->used_R_MapTable_entris = 0;
            ssd->used_R_MapTable_parity_entris = 0;
            ssd->valid_RMMs = 0;
            ssd->valid_RMM_pages = 0;
            ssd->cpu_cycle_tt = 0;
            uint64_t current_time = qemu_clock_get_ns(QEMU_CLOCK_REALTIME);
            current_time = (current_time > ssd->next_ssd_avail_time ? current_time : ssd->next_ssd_avail_time);
            my_log(ssd->fp_info, "%s:%lu\n", "TestBegin", current_time/1e9);
            return 0;
        }
        else if(req->special_value == -2) {
            ssd->test_begin = false;
            uint64_t current_time = qemu_clock_get_ns(QEMU_CLOCK_REALTIME);
            printf_info(ssd, true);
            current_time = (current_time > ssd->next_ssd_avail_time ? current_time : ssd->next_ssd_avail_time);
            my_log(ssd->fp_info, "%s:%lu\n", "TestEnd", current_time/1e9);
            return 0;
        }
        else {
            ssd->metadata_offset = req->slba / ssd->sp.secs_per_pg;
            my_log(ssd->fp_info, "%s:%lu, Offset(sector):%lu(%x), Offset(LPN):%lu(%x), special_value:%lu\n", "RemoteInit", 
                    req->global_slba, req->slba, req->slba, ssd->metadata_offset, ssd->metadata_offset, req->special_value);
            ssd_init_remote_maptbl(ssd, req->special_value);
            return 0;
        }
    }

    ssd->tt_trims[TOTAL]++;
    ssd->tt_trims[LAST_SECOND]++;

    struct ssdparams *spp = &ssd->sp;
    uint64_t lpn = req->is_remote_slba ? req->remote_entry_offset : req->slba / spp->secs_per_pg;
    my_assert(ssd, lpn < spp->tt_pgs, "is_remote:%d, lpn:%d, entry_offset:%d, tt_pgs:%d", 
        req->is_remote_slba, req->slba / spp->secs_per_pg, req->remote_entry_offset, spp->tt_pgs);
    struct ppa ppa = get_maptbl_ent(ssd, lpn, req->is_remote_slba);
    if(!mapped_ppa(&ppa))  return 0;

    // if(local_lpn == 181675 || global_lpn == 593324 || (global_lpn % spp->tt_remote_pgs == 593324 % spp->tt_remote_pgs)) {
    //     my_log(ssd->fp_info, "TRIM: lpn:%d, ppa=%lu, is_remote=%d, global_lpn=%d, local_lpn=%d\n", lpn, ppa.ppa, req->is_remote_slba, global_lpn, local_lpn);
    // }

    // my_log(ssd->fp_info, "TRIM: local_lpn:%d(%x), global_lpn:%d(%x), is_remote_slba:%d, ppa:%d(%x)\n", 
    //     local_lpn, local_lpn, global_lpn, global_lpn, req->is_remote_slba, ppa.ppa, ppa.ppa);

    my_assert(ssd, mapped_ppa(&ppa) && valid_ppa(ssd, &ppa), 
        "is_remote:%d, lpn:%d(%x), entry_offset:%d, tt_pgs:%d, ppa:%d(%x)", 
        req->is_remote_slba, req->slba / spp->secs_per_pg, req->slba / spp->secs_per_pg, req->remote_entry_offset, spp->tt_pgs, ppa.ppa, ppa.ppa);

    struct rmap_elem elem;
    elem.lpn = lpn;
    elem.RMM_page_p = NULL;
    delete_reference(ssd, req->is_remote_slba, &ppa, elem);

    printf_info(ssd, false);
    return 0;
}

inline void printf_info(struct ssd *ssd, bool force_print)
{
    int time_s = ssd->next_ssd_avail_time / 1e9;
    if (force_print || (ssd->test_begin && time_s > ssd->last_print_time_s)) {
        ssd->last_print_time_s = time_s;
        my_log(ssd->fp_info, "time:%d, \
            tt_IOs:(%d,%d),(%d,%d),(%d,%d),(%d,%d), \
            tt_GC_IOs:(%d,%d,%d),(%d,%d,%d), \
            tt_RMM_IOs:(%d,%d),(%d,%d), \
            tt_remaps:%d(%d), \
            R_MapTable_entris:%d(%d), RMMs:%d, RMM_pages:%d, \
            TRIMs:%d(%d), \
            metadata_pages:%d(%.2fGB), user_pages:%d(%.2fGB), RMM_pages:%d(%.2fGB),\
            RMM_pages:(malloc,%d)(free,%d),\
            cpu_cycle_tt:%ld\n",
            time_s, 
            ssd->tt_IOs[TOTAL][NAND_READ][USER_IO], ssd->tt_IOs[TOTAL][NAND_WRITE][USER_IO], ssd->tt_IOs[LAST_SECOND][NAND_READ][USER_IO], ssd->tt_IOs[LAST_SECOND][NAND_WRITE][USER_IO], ssd->tt_IOs[TOTAL][NAND_READ][METADATA_IO], ssd->tt_IOs[TOTAL][NAND_WRITE][METADATA_IO], ssd->tt_IOs[LAST_SECOND][NAND_READ][METADATA_IO], ssd->tt_IOs[LAST_SECOND][NAND_WRITE][METADATA_IO],
            ssd->tt_GC_IOs[TOTAL][NAND_READ], ssd->tt_GC_IOs[TOTAL][NAND_WRITE], ssd->tt_GC_IOs[TOTAL][NAND_ERASE], ssd->tt_GC_IOs[LAST_SECOND][NAND_READ], ssd->tt_GC_IOs[LAST_SECOND][NAND_WRITE], ssd->tt_GC_IOs[LAST_SECOND][NAND_ERASE],
            ssd->tt_RMM_IOs[TOTAL][NAND_READ], ssd->tt_RMM_IOs[TOTAL][NAND_WRITE], ssd->tt_RMM_IOs[LAST_SECOND][NAND_READ], ssd->tt_RMM_IOs[LAST_SECOND][NAND_WRITE],
            ssd->tt_remaps[TOTAL], ssd->tt_remaps[LAST_SECOND], 
            ssd->used_R_MapTable_entris, ssd->used_R_MapTable_parity_entris, ssd->valid_RMMs, ssd->valid_RMM_pages,
            ssd->tt_trims[TOTAL], ssd->tt_trims[LAST_SECOND],
            ssd->type_page_count[metapage], (float)ssd->type_page_count[metapage]*4/1024/1024, 
            ssd->type_page_count[userpage], (float)ssd->type_page_count[userpage]*4/1024/1024,
            ssd->type_page_count[RMMpage], (float)ssd->type_page_count[RMMpage]*4/1024/1024,
            ssd->g_malloc_RMM_pages, ssd->g_free_RMM_pages,
            ssd->cpu_cycle_tt);

        ssd->tt_IOs[LAST_SECOND][NAND_READ][USER_IO] = ssd->tt_IOs[LAST_SECOND][NAND_WRITE][USER_IO] = ssd->tt_IOs[LAST_SECOND][NAND_READ][METADATA_IO] = ssd->tt_IOs[LAST_SECOND][NAND_WRITE][METADATA_IO] = 0;
        ssd->tt_GC_IOs[LAST_SECOND][NAND_READ] = ssd->tt_GC_IOs[LAST_SECOND][NAND_WRITE] = ssd->tt_GC_IOs[LAST_SECOND][NAND_ERASE] = 0;
        ssd->tt_RMM_IOs[LAST_SECOND][NAND_READ] = ssd->tt_RMM_IOs[LAST_SECOND][NAND_WRITE] = 0;
        ssd->tt_remaps[LAST_SECOND] = 0;
        ssd->tt_trims[LAST_SECOND] = 0;
    }
}

/* wen:added  Remap-SSD */
inline void add_reference(struct ssd *ssd, bool if_remote_lpn, struct ppa *ppa, bool if_rmap, struct rmap_elem elem)
{
    struct line *line = get_line(ssd, ppa);

    struct nand_page *pg = NULL;
    pg = get_pg(ssd, ppa);
    my_assert(ssd, pg != NULL, "Wrong add_reference 1!\n");
    my_assert(ssd, pg->refcount >= 0, "Wrong add_reference 2!\n");
    if(pg->refcount++ ==0) {
        mark_page_valid(ssd, ppa);
        my_assert(ssd, get_rmap_ent(ssd, ppa).lpn == INVALID_LPN, "Wrong add_reference, there is a rmap(lpn=%d, tt_pgs=%d) in this ppa!\n", get_rmap_ent(ssd, ppa).lpn, ssd->sp.tt_pgs);
        ssd->type_page_count[line->is_RMM_line ? RMMpage : if_remote_lpn ? userpage : elem.lpn >= ssd->metadata_offset ? userpage : metapage]++;
    }

    set_maptbl_ent(ssd, elem.lpn, if_remote_lpn, ppa);
    if(if_rmap)
        set_rmap_ent(ssd, elem, ppa);

    if(if_remote_lpn || elem.lpn >= ssd->sp.tt_pgs) {
        if(elem.lpn >= ssd->sp.tt_pgs)
            ssd->used_R_MapTable_parity_entris++;
        else 
            ssd->used_R_MapTable_entris++;
    }

    debug_log(ssd->fp_debug_info, "add_reference: if_remote_lpn:%s != elem.lpn(%d) or + tt_pgs(%d), cur_lpn(%d), ppa(%d)\n", 
        if_remote_lpn ? "true" : "false", elem.lpn, ssd->sp.tt_pgs, get_rmap_ent(ssd, ppa).lpn, ppa->ppa);
}

inline void delete_reference(struct ssd *ssd, bool if_remote_lpn, struct ppa *ppa, struct rmap_elem elem)
{
    //lpn is in M-MapTable if if_remote_lpn is false, otherwise, lpn is a global lpn in R-MapTable
    struct line *line = get_line(ssd, ppa);

    struct nand_page *pg = NULL;
    pg = get_pg(ssd, ppa);
    my_assert(ssd, pg != NULL, "Wrong delete_reference 1!\n");
    my_assert(ssd, pg->refcount > 0, "Wrong delete_reference 2, ref=%d!\n", pg->refcount);
    if(--pg->refcount == 0) {
        mark_page_invalid(ssd, ppa);
        ssd->type_page_count[line->is_RMM_line ? RMMpage : if_remote_lpn ? userpage : elem.lpn >= ssd->metadata_offset ? userpage : metapage]--;
    }

    struct rmap_elem old_elem = get_rmap_ent(ssd, ppa); 
    if(line->is_RMM_line) {
        my_assert(ssd, elem.lpn == RMM_PAGE, "Error: it is a RMM_Line but elem.lpn != RMM_PAGE");
        my_assert(ssd, elem.RMM_page_p != NULL, "Error: it is a RMM_PAGE but elem.RMM_page_p == NULL");
        struct rmap_elem new_elem; 
        new_elem.lpn = INVALID_LPN;
        new_elem.RMM_page_p = NULL;
        set_rmap_ent(ssd, new_elem, ppa);
    }
    else if(!if_remote_lpn && old_elem.lpn == elem.lpn) {
        my_assert(ssd, elem.lpn != RMM_PAGE, "Error: it is not a RMM_Line but elem.lpn == RMM_PAGE");
        my_assert(ssd, elem.lpn != INVALID_LPN, "Error: it is not a RMM_Line but elem.lpn == INVALID_LPN");
        struct rmap_elem new_elem; 
        new_elem.lpn = INVALID_LPN;
        new_elem.RMM_page_p = NULL;
        set_rmap_ent(ssd, new_elem, ppa);
    }
    else if(old_elem.lpn != INVALID_LPN && old_elem.lpn >= ssd->sp.tt_pgs) {        //remote parity
        my_assert(ssd, old_elem.lpn == elem.lpn + ssd->sp.tt_pgs || old_elem.lpn == elem.lpn, 
            "Error: ref_decrease but old_elem.lpn(%d, if_remote_lpn:%s) != elem.lpn(%d) or + tt_pgs(%d)", 
            old_elem.lpn, if_remote_lpn ? "true" : "false", elem.lpn, ssd->sp.tt_pgs);
        struct rmap_elem new_elem; 
        new_elem.lpn = INVALID_LPN;
        new_elem.RMM_page_p = NULL;
        set_rmap_ent(ssd, new_elem, ppa);
    }
    //otherwise, it is a remap-lpn, its rmap is in RMM(updated in segment_migration) instead of rmap_ent(updated here)

    struct ppa unmapped_ppa;
    unmapped_ppa.ppa = UNMAPPED_PPA;
    set_maptbl_ent(ssd, elem.lpn, if_remote_lpn, &unmapped_ppa);

    if(if_remote_lpn || (elem.lpn >= ssd->sp.tt_pgs && (elem.lpn - ssd->sp.tt_pgs) <= ssd->sp.tt_remote_pgs) || old_elem.lpn == elem.lpn + ssd->sp.tt_pgs) {
        if(elem.lpn >= ssd->sp.tt_pgs || old_elem.lpn == elem.lpn + ssd->sp.tt_pgs)
            ssd->used_R_MapTable_parity_entris--;
        else 
            ssd->used_R_MapTable_entris--;
    }

    debug_log(ssd->fp_debug_info, "delete_reference: old_elem.lpn(%d, if_remote_lpn:%s) != elem.lpn(%d) or + tt_pgs(%d), cur_lpn(%d), ppa(%d)\n", 
            old_elem.lpn, if_remote_lpn ? "true" : "false", elem.lpn, ssd->sp.tt_pgs, get_rmap_ent(ssd, ppa).lpn, ppa->ppa);

    if(pg->refcount == 0) {
        my_assert(ssd, get_rmap_ent(ssd, ppa).lpn == INVALID_LPN, 
            "Wrong delete_reference 3! old_elem.lpn(%d, if_remote_lpn:%s) != elem.lpn(%d) or + tt_pgs(%d), cur_lpn(%d), ppa(%d)\n", 
            old_elem.lpn, if_remote_lpn ? "true" : "false", elem.lpn, ssd->sp.tt_pgs, get_rmap_ent(ssd, ppa).lpn, ppa->ppa);
    }
}

inline uint64_t ppa_to_OffsetInLine(struct ssd *ssd, struct ppa *ppa)
{
    struct ssdparams *spp = &ssd->sp;
    uint64_t res = (ppa->g.ch * spp->luns_per_ch + ppa->g.lun) * spp->pgs_per_blk + ppa->g.pg;
    my_assert(ssd, res < ssd->sp.pgs_per_line, "error in ppa_to_offsetinline: res=%lu, ppa=%lu\n", res, ppa->ppa);
    return res;
}

inline struct ppa OffsetInLine_to_ppa(struct ssd *ssd, uint64_t offset, uint64_t line_id)
{
    struct ssdparams *spp = &ssd->sp;
    struct ppa ppa;
    ppa.ppa = 0;
    ppa.g.ch = (offset / spp->pgs_per_blk) / spp->luns_per_ch;
    ppa.g.lun = (offset / spp->pgs_per_blk) % spp->luns_per_ch;
    ppa.g.pl = 0;      //TODO: multi-plane
    ppa.g.blk = line_id;
    ppa.g.pg = offset % spp->pgs_per_blk;
    bool is_valid_ppa = valid_ppa(ssd, &ppa);
    my_assert(ssd, is_valid_ppa, "error in OffsetInLine_to_ppa: offset=%ld, line_id=%d, ppa=%lu, pgs_per_blk=%d, luns_per_ch=%d, %d %d %d %d %d %d\n", 
    offset, line_id, ppa.ppa, spp->pgs_per_blk, spp->luns_per_ch, ppa.g.ch, ppa.g.lun, ppa.g.pl, ppa.g.blk, ppa.g.pg, ppa.g.sec);
    return ppa;
}

inline struct RMM_page *get_RMM_page(struct ssd *ssd)
{
    struct RMM_page *RMM_page = g_malloc0(sizeof(struct RMM_page));
    ssd->g_malloc_RMM_pages++;
    RMM_page->ppa = get_new_page(ssd, true);
    return RMM_page;
}

void flush_RMM_page(struct ssd *ssd, struct RMM_page *RMM_page)
{
    struct rmap_elem elem;
    elem.lpn = RMM_PAGE;
    elem.RMM_page_p = RMM_page;

    struct ppa *ppa = &RMM_page->ppa;
    add_reference(ssd, false, ppa, true, elem);
    ssd_advance_write_pointer(ssd, true);

    struct nand_cmd nvrd;
    nvrd.stime = 0;
    nvrd.cmd = NVRAM_READ;
    nvram_advance_status(ssd, &nvrd, Segment_Per_Page*RMMs_Per_Segment);

    struct nand_cmd swr;
    swr.stime = 0;
    swr.cmd = NAND_WRITE;
    ssd_advance_status(ssd, ppa, &swr);

    ssd->tt_RMM_IOs[TOTAL][NAND_WRITE]++;
    ssd->tt_RMM_IOs[LAST_SECOND][NAND_WRITE]++;
    ssd->valid_RMM_pages++;
}

inline void mark_segment_free(struct ssd *ssd, struct segment *segment)
{
    memset(segment, 0, sizeof(struct segment));
    segment->metadata.line_id = UNALLOCATED_SEGMENT;
    QTAILQ_INSERT_TAIL(&ssd->segment_management.free_segment_list, segment, entry);
    ssd->segment_management.free_segment_cnt++;
}

inline void read_RMM_page(struct ssd *ssd, struct ppa *ppa)
{
    ssd->tt_RMM_IOs[TOTAL][NAND_READ]++;
    ssd->tt_RMM_IOs[LAST_SECOND][NAND_READ]++;
    struct nand_cmd srd;
    srd.cmd = NAND_READ;
    srd.stime = 0;
    ssd_advance_status(ssd, ppa, &srd);
}

void ssd_init_remote_maptbl(struct ssd *ssd, uint64_t R_MapTable_size)
{
    struct ssdparams *spp = &ssd->sp;
    spp->tt_remote_pgs = R_MapTable_size;
    if(R_MapTable_size > 0) {
        ssd->remote_maptbl = g_malloc0(sizeof(struct ppa) * spp->tt_remote_pgs);
        for (int i = 0; i < spp->tt_remote_pgs; i++) {
            ssd->remote_maptbl[i].ppa = UNMAPPED_PPA;
        }
    }
}

void ssd_init_segments(struct ssd *ssd)
{
    struct segment_mgmt *segment_management = &ssd->segment_management;
    segment_management->segments = g_malloc0(sizeof(struct segment) * NVRAM_Segment_Count);
    segment_management->free_segment_cnt = 0;
    QTAILQ_INIT(&segment_management->free_segment_list);
    struct segment *segment;
    for (int i = 0; i < NVRAM_Segment_Count; i++) {
        segment = &segment_management->segments[i];
        mark_segment_free(ssd, segment);
    }
    assert(segment_management->free_segment_cnt == NVRAM_Segment_Count);
}

void ssd_init_write_RMM_pointer(struct ssd *ssd)
{
    struct write_pointer *wpp = &ssd->wp_RMM;
    struct line_mgmt *lm = &ssd->lm;
    struct line *curline = NULL;
    /* make sure lines are already initialized by now */
    curline = QTAILQ_FIRST(&lm->free_line_list);
    QTAILQ_REMOVE(&lm->free_line_list, curline, entry);
    lm->free_line_cnt--;
    /* wpp->curline is always our onging line for writes */
    wpp->curline = curline;
    wpp->ch = 0;
    wpp->lun = 0;
    wpp->pg = 0;
    wpp->blk = curline->id;;
    wpp->pl = 0;

    curline->is_RMM_line = true;
}

inline void ssd_init_GC_migration_mappins(struct ssd *ssd)
{
    if(ssd->GC_migration_mappings == NULL)
        ssd->GC_migration_mappings = g_malloc0(sizeof(struct ppa) * ssd->sp.pgs_per_line);
    for(int i=0; i<ssd->sp.pgs_per_line; i++)
        ssd->GC_migration_mappings[i].ppa = UNMAPPED_PPA;
    ssd->wait_migrate_RMMs = ssd->do_migrate_RMMs = 0;
}

//SSD-GC needs to read RMMs for updating P2L mappings, thus we firstly flush RMMs of line which won't be GC (has the most of valid pages)
int do_NVRAM_gc(struct ssd *ssd, struct line* target_line)
{
    struct segment_mgmt *sm = &ssd->segment_management;
    struct line_mgmt *lm = &ssd->lm;
    struct line *line, *victim_line = NULL;
    struct segment* segment;
    int min_ipc = INT_MAX;
    for(int i=0; i<NVRAM_Segment_Count; i++) {
        segment = &sm->segments[i];
        if(segment->metadata.line_id != UNALLOCATED_SEGMENT) {
            my_assert(ssd, segment->metadata.line_id < ssd->sp.tt_lines, "Error: segment->metadata.line_id >= ssd->sp.tt_lines");
            line = &lm->lines[segment->metadata.line_id];
            if (line->segment_count_inNVRAM >= Segment_Per_Page && line->ipc < min_ipc) {
                victim_line = line;
                min_ipc = line->ipc;
            }
        }
    }
    if(min_ipc == INT_MAX) {
        for(int i=0; i<NVRAM_Segment_Count; i++) {
            segment = &sm->segments[i];
            if(segment->metadata.line_id != UNALLOCATED_SEGMENT) {
                my_assert(ssd, segment->metadata.line_id < ssd->sp.tt_lines, "Error: segment->metadata.line_id >= ssd->sp.tt_lines");
                line = &lm->lines[segment->metadata.line_id];
                if (line->segment_count_inNVRAM > 0) {
                    victim_line = line;
                    break;
                }
            }
        }
    }
    assert(victim_line != NULL);

    int released_segment_count = 0;
    struct RMM_page *RMM_page = get_RMM_page(ssd);
    QTAILQ_FOREACH(segment, &victim_line->segment_group_list, entry) {
        memcpy(&(RMM_page->segments[released_segment_count]), segment, sizeof(struct segment));
        if(++released_segment_count == Segment_Per_Page)
            break;
    }
    QTAILQ_INSERT_TAIL(&victim_line->RMM_page_list, RMM_page, entry);
    flush_RMM_page(ssd, RMM_page);

    for(int i=released_segment_count; i>0; i--) {
        segment = QTAILQ_FIRST(&victim_line->segment_group_list);
        QTAILQ_REMOVE(&victim_line->segment_group_list, segment, entry);
        mark_segment_free(ssd, segment);
        victim_line->segment_count_inNVRAM--;
    }

    // struct timespec t;
    // t.tv_sec = 0;
    // t.tv_nsec = (nvram_rd_lat_per_RMM + nvram_wr_lat_per_RMM) * migrated_RMM_count;
    // nanosleep(&t, NULL);
    return released_segment_count;
}

struct RMM* get_RMM(struct ssd *ssd, struct line* line)
{
    struct segment_mgmt *segment_management = &ssd->segment_management;
    struct segment *segment = NULL;
    segment = QTAILQ_LAST(&line->segment_group_list, segment_group_list);
    if(segment == NULL || segment->metadata.RMM_number == RMMs_Per_Segment)
    {
        while (QTAILQ_EMPTY(&segment_management->free_segment_list)) {
            if(do_NVRAM_gc(ssd, line) > 0)
                break;
        }

        segment = QTAILQ_FIRST(&segment_management->free_segment_list);
        QTAILQ_REMOVE(&segment_management->free_segment_list, segment, entry);
        segment_management->free_segment_cnt--;

        segment->metadata.line_id = line->id;
        segment->metadata.segment_number = line->next_segment_id++;
        QTAILQ_INSERT_TAIL(&line->segment_group_list, segment, entry);
        line->segment_count_inNVRAM++;
    }
    assert(segment != NULL && segment->metadata.RMM_number < RMMs_Per_Segment);
    ssd->valid_RMMs++;
    return &segment->RMMs[segment->metadata.RMM_number++];
}

void RMM_migration(struct ssd *ssd, struct RMM *RMM, int line_id)
{
    struct ppa old_ppa = OffsetInLine_to_ppa(ssd, RMM->offset, line_id);
    // struct ppa old_ppa2 = get_maptbl_ent(ssd, RMM->target_LPN, RMM->if_remote_lpn);
    // my_assert(ssd, old_ppa.ppa == old_ppa2.ppa,
    //     "error RMM-migrateion: offset=%d, line_id=%d, old_ppa=%d, LPN=%d, if_remote=%d, old_ppa=%d\n",
    //     RMM->offset, line_id, old_ppa.ppa, RMM->target_LPN, RMM->if_remote_lpn, old_ppa2.ppa);

    struct ppa new_ppa = ssd->GC_migration_mappings[RMM->offset];
    my_assert(ssd, new_ppa.ppa != UNMAPPED_PPA, "Error: new_ppa == UNMAPPED_PPA in RMM_migration()\n");
    
    /* update maptbl */
    struct rmap_elem elem;
    elem.lpn = RMM->target_LPN;
    elem.RMM_page_p = NULL;
    delete_reference(ssd, RMM->if_remote_lpn, &old_ppa, elem);

    if(new_ppa.ppa == DELAY_WRITE) {
        new_ppa = get_new_page(ssd, false);
        ssd->GC_migration_mappings[RMM->offset] = new_ppa;
        add_reference(ssd, RMM->if_remote_lpn, &new_ppa, false, elem);
        ssd_advance_write_pointer(ssd, false);

        ssd->tt_GC_IOs[TOTAL][NAND_WRITE]++;
        ssd->tt_GC_IOs[LAST_SECOND][NAND_WRITE]++;
        struct nand_cmd gcw;
        gcw.cmd = NAND_WRITE;
        gcw.stime = 0;
        ssd_advance_status(ssd, &new_ppa, &gcw);
    }
    else{
        add_reference(ssd, RMM->if_remote_lpn, &new_ppa, false, elem);
    }

    /* update rmap of target_lpn in RMMs */
    struct RMM *new_RMM = NULL;
    new_RMM = get_RMM(ssd, get_line(ssd, &new_ppa));
    my_assert(ssd, new_RMM != NULL, "Error: new_RMM is null");
    new_RMM->torn_bit_0 = 1;
    new_RMM->offset = ppa_to_OffsetInLine(ssd, &new_ppa);
    new_RMM->number = ssd->cur_write_number++;
    new_RMM->torn_bit_1 = 1;
    new_RMM->if_remote_lpn = RMM->if_remote_lpn;
    new_RMM->target_LPN = RMM->target_LPN;
    my_assert(ssd, is_valid_RMM(ssd, new_RMM, get_line(ssd, &new_ppa)->id), "Error: it is an invalid RMM");
    ssd->do_migrate_RMMs++;

    struct nand_cmd nvwr;
    nvwr.stime = 0;
    nvwr.cmd = NVRAM_WRITE;
    nvram_advance_status(ssd, &nvwr, 1);
}

inline bool is_valid_RMM(struct ssd *ssd, struct RMM *RMM, int line_id)
{
    struct ppa ppa = get_maptbl_ent(ssd, RMM->target_LPN, RMM->if_remote_lpn);
    if(mapped_ppa(&ppa) && ppa.ppa == OffsetInLine_to_ppa(ssd, RMM->offset, line_id).ppa) {
        my_assert(ssd, get_pg(ssd, &ppa)->refcount > 0, 
            "error judge whether is a valid RMM: ppa=%d, ref=%d, lpn=%d, if_remote=%d, offset=%d, segment_line=%d\n",
            ppa.ppa, get_pg(ssd, &ppa)->refcount, RMM->target_LPN, RMM->if_remote_lpn, RMM->offset, line_id);
        return true;
    }
    return false;
}

void segment_migration(struct ssd *ssd, struct segment *segment)
{
    for(int RMM_id=0; RMM_id<segment->metadata.RMM_number; RMM_id++) {
        if(is_valid_RMM(ssd, &segment->RMMs[RMM_id], segment->metadata.line_id))
            RMM_migration(ssd, &segment->RMMs[RMM_id], segment->metadata.line_id);
    }
}

void segmentgroup_migration(struct ssd *ssd, struct line *line)
{
    struct segment *segment;
    QTAILQ_FOREACH(segment, &line->segment_group_list, entry) {
        my_assert(ssd, segment->metadata.line_id == line->id, 
        "error segmentgroup_migration, segment_line:%d, migrate_line:%d\n",
        segment->metadata.line_id, line->id);
        struct nand_cmd nvrd;
        nvrd.stime = 0;
        nvrd.cmd = NVRAM_READ;
        nvram_advance_status(ssd, &nvrd, RMMs_Per_Segment);
        segment_migration(ssd, segment);
    }

    while(!QTAILQ_EMPTY(&line->segment_group_list)) {
        segment = QTAILQ_FIRST(&line->segment_group_list);
        QTAILQ_REMOVE(&line->segment_group_list, segment, entry);
        mark_segment_free(ssd, segment);
        ssd->valid_RMMs -= RMMs_Per_Segment;
        line->segment_count_inNVRAM--;
    }

    struct RMM_page* RMM_page;
    QTAILQ_FOREACH(RMM_page, &line->RMM_page_list, entry) {
        read_RMM_page(ssd, &RMM_page->ppa);
        for(int segment_id=0; segment_id<Segments_Per_Page; segment_id++) {
            segment_migration(ssd, &RMM_page->segments[segment_id]);
        }
    }

    while(!QTAILQ_EMPTY(&line->RMM_page_list)) {
        RMM_page = QTAILQ_FIRST(&line->RMM_page_list);
        QTAILQ_REMOVE(&line->RMM_page_list, RMM_page, entry);
        struct rmap_elem elem;
        elem.lpn = RMM_PAGE;
        elem.RMM_page_p = RMM_page;
        delete_reference(ssd, false, &RMM_page->ppa, elem);
        g_free(RMM_page);
        ssd->valid_RMM_pages--;
        ssd->g_free_RMM_pages++;
    }

    my_assert(ssd, ssd->wait_migrate_RMMs == ssd->do_migrate_RMMs, "Error: ssd->wait_migrate_RMMs(%d) != do_migrate_RMMs(%d)", 
        ssd->wait_migrate_RMMs, ssd->do_migrate_RMMs);
}