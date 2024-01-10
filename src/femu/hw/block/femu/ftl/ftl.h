#ifndef __FTL_H
#define __FTL_H

#include "qemu/osdep.h"
#include "qemu/thread.h"

#define INVALID_PPA     (~(0ULL))
#define INVALID_LPN     (~(0ULL))
#define UNMAPPED_PPA    (~(0ULL))
#define UNALLOCATED_SEGMENT    (~(0ULL))
#define RMM_PAGE        (~(1ULL))
#define UNKNOW          (~(0ULL))
#define DELAY_WRITE     (~(1ULL))

enum {
    NAND_READ =  0,
    NAND_WRITE = 1,
    NAND_ERASE = 2,
    NVRAM_READ = 3,
    NVRAM_WRITE = 4
};

enum {
    TOTAL =  0,
    LAST_SECOND = 1
};

enum {
    USER_IO = 0,
    METADATA_IO = 1
};

enum {
    SEC_FREE = 0,
    SEC_INVALID = 1,
    SEC_VALID = 2,

    PG_FREE = 0,
    PG_INVALID = 1,
    PG_VALID = 2
};

#define BLK_BITS    (16)
#define PG_BITS     (16)
#define SEC_BITS    (8)
#define PL_BITS     (8)
#define LUN_BITS    (8)
#define CH_BITS     (7)

/* wen:added  Remapping metadata entry of Remap-SSD */
#define free_type 0
#define used_type 1
#define valid_type 2
#define metapage 0
#define userpage 1
#define RMMpage 2

#define KB      (1024)
#define MB      (1024*KB)
#define GB      (1024*MB)

#define nvram_rd_lat_per_RMM  ((50)*64/RMM_Size) /* 50ns per 64B */
#define nvram_wr_lat_per_RMM  ((500)*64/RMM_Size) /* 500ns per 64B */

#define RMM_Size        (16)     //16Bytes
#define Segment_Size    (1*KB)
#define Page_Size       (4*KB)
#define NVRAM_Size      (2*MB)
// #define NVRAM_Size      (1*GB)
struct RMM{
    uint64_t torn_bit_0:        1;
    uint64_t offset:            21;
    uint64_t number:            42;

    uint64_t torn_bit_1:        1;
    uint64_t if_remote_lpn:     1;
    uint64_t target_LPN:        62;
};

#define RMMs_Per_Segment ((Segment_Size/RMM_Size)-1)
struct segment{
    struct {
        uint64_t line_id:           32;
        uint64_t RMM_number:        32;
        uint64_t segment_number:    32;
        //uint64_t next_segment_id:   32;   //simply adopt the QTAILQ structure
    } metadata;
    struct RMM RMMs[RMMs_Per_Segment];
    //unsigned long bitarray[BITS_TO_LONGS(RMMs_Per_Segment)];    //need only when GC
    QTAILQ_ENTRY(segment) entry;        //simply adopt the QTAILQ structure, or use the metadata.next_segment_id for list
};

#define Segment_Per_Page (Page_Size/Segment_Size)
#define NVRAM_Segment_Count (NVRAM_Size/Segment_Size)
struct segment_mgmt {
    struct segment* segments;
    /* free segment list, we only need to maintain a list of segment numbers */
    QTAILQ_HEAD(free_segment_list, segment) free_segment_list;
    int free_segment_cnt;
};


/* describe a physical page addr */
struct ppa {
    union {
        struct {
            uint64_t blk : BLK_BITS;
            uint64_t pg  : PG_BITS;
            uint64_t sec : SEC_BITS;
            uint64_t pl  : PL_BITS;
            uint64_t lun : LUN_BITS;
            uint64_t ch  : CH_BITS;
            uint64_t rsv : 1;
        } g;

        uint64_t ppa;
    };
};

#define Segments_Per_Page (Page_Size/Segment_Size)
struct RMM_page{
    struct segment segments[Segments_Per_Page];
    struct ppa ppa;
    QTAILQ_ENTRY(RMM_page) entry;
};

typedef int nand_sec_status_t;

struct nand_page {
    nand_sec_status_t *sec;
    int nsecs;
    int status;
    int refcount;
};

struct nand_block {
    struct nand_page *pg;
    int npgs;
    int ipc; /* invalid page count */
    int vpc; /* valid page count */
    int erase_cnt;
    int wp; /* current write pointer */
};

struct nand_plane {
    struct nand_block *blk;
    int nblks;
};

struct nand_lun {
    struct nand_plane *pl;
    int npls;
    uint64_t next_lun_avail_time;
    bool busy;
    int gc_counter;
    int gc_page_counter;
};

struct ssd_channel {
    struct nand_lun *lun;
    int nluns;
    uint64_t next_ch_avail_time;
    bool busy;
};

struct ssdparams {
    int secsz;        /* sector size in bytes */
    int secs_per_pg;  /* # of sectors per page */
    int pgs_per_blk;  /* # of NAND pages per block */
    int blks_per_pl;  /* # of blocks per plane */
    int pls_per_lun;  /* # of planes per LUN (Die) */
    int luns_per_ch;  /* # of LUNs per channel */
    int nchs;         /* # of channels in the SSD */

    int pg_rd_lat;    /* NAND page read latency in nanoseconds */
    int pg_wr_lat;    /* NAND page program latency in nanoseconds */
    int blk_er_lat;   /* NAND block erase latency in nanoseconds */
    int ch_xfer_lat;  /* channel transfer latency for one page in nanoseconds
                       * this defines the channel bandwith
                       */

    double gc_thres_pcent;
    int gc_thres_lines;
    double gc_thres_pcent_high;
    int gc_thres_lines_high;

    /* below are all calculated values */
    int secs_per_blk; /* # of sectors per block */
    int secs_per_pl;  /* # of sectors per plane */
    int secs_per_lun; /* # of sectors per LUN */
    int secs_per_ch;  /* # of sectors per channel */
    int tt_secs;      /* # of sectors in the SSD */

    int pgs_per_pl;   /* # of pages per plane */
    int pgs_per_lun;  /* # of pages per LUN (Die) */
    int pgs_per_ch;   /* # of pages per channel */
    int tt_pgs;       /* total # of pages in the SSD */

    int blks_per_lun; /* # of blocks per LUN */
    int blks_per_ch;  /* # of blocks per channel */
    int tt_blks;      /* total # of blocks in the SSD */

    int secs_per_line;
    int pgs_per_line;
    int blks_per_line;
    int tt_lines;

    int pls_per_ch;   /* # of planes per channel */
    int tt_pls;       /* total # of planes in the SSD */

    int tt_luns;      /* total # of LUNs in the SSD */

    int tt_remote_pgs;
};

typedef struct line {
    int id;  /* line id, the same as corresponding block id */
    int ipc; /* invalid page count in this line */
    int vpc; /* valid page count in this line */
    QTAILQ_ENTRY(line) entry; /* in either {free,victim,full} list */
    /* wen:added  Remap-SSD */
    QTAILQ_HEAD(segment_group_list, segment) segment_group_list;    //stored in NVRAM
    int next_segment_id;
    QTAILQ_HEAD(RMM_page_list, RMM_page) RMM_page_list;      //stored in flash
    bool is_RMM_line;
    int segment_count_inNVRAM;
} line;

/* wp: record next write addr */
struct write_pointer {
    struct line *curline;
    int ch;
    int lun;
    int pg;
    int blk;
    int pl;
};

struct line_mgmt {
    struct line *lines;
    /* free line list, we only need to maintain a list of blk numbers */
    QTAILQ_HEAD(free_line_list, line) free_line_list;
    QTAILQ_HEAD(victim_line_list, line) victim_line_list;
    QTAILQ_HEAD(full_line_list, line) full_line_list;
    int tt_lines;
    int free_line_cnt;
    int victim_line_cnt;
    int full_line_cnt;
};

struct nand_cmd {
    int cmd;
    int64_t stime; /* Coperd: request arrival time */
};

struct rmap_elem {
    uint64_t lpn;
    struct RMM_page* RMM_page_p;
};

struct ssd {
    char *ssdname;
    struct ssdparams sp;
    struct ssd_channel *ch;
    struct ppa *maptbl; /* page level mapping table */
    struct rmap_elem *rmap;     /* reverse mapptbl, assume it's stored in OOB */
    struct write_pointer wp;
    struct line_mgmt lm;
    uint16_t id; /* unique id for synchronization */
    uint64_t next_ssd_avail_time;

    /* lockless ring for communication with NVMe IO thread */
    struct rte_ring *to_ftl;
    struct rte_ring *to_poller;
    bool *dataplane_started_ptr;
    QemuThread ftl_thread;

    /* wen:added  Remap-SSD */
    uint64_t next_NVRAM_avail_time;
    uint64_t cur_write_number;
    int last_print_time_s;
    struct segment_mgmt segment_management;
    struct write_pointer wp_RMM;
    struct ppa *remote_maptbl;  /* X-to-1 direct mapping table of remote LPN space */
    struct femu_mbe remote_parity_mbe;
    struct ppa *GC_migration_mappings; //records the mapping between old_ppa and new_ppa when GC, for updating RMMs

    //statistics
    char info_file_name[100];
    FILE* fp_info;
    FILE* fp_debug_info;
    char latency_file_name[100];
    FILE* fp_latency;
    uint64_t metadata_offset;
    uint64_t type_page_count[3];    //[metadata, userdata, RMM]
    bool test_begin;    //begin to collect statistic information
    uint64_t tt_IOs[2][2][2];   //[TOTAL, LAST_SECOND][READ, WRITE][User, Metadata]
    uint64_t tt_GC_IOs[2][3];
    uint64_t tt_RMM_IOs[2][2];
    uint64_t tt_remaps[2];
    uint64_t tt_trims[2];
    uint64_t used_R_MapTable_entris;
    uint64_t used_R_MapTable_parity_entris;
    uint64_t valid_RMMs;
    uint64_t valid_RMM_pages;
    uint64_t cpu_cycle_tt;

    uint64_t g_malloc_RMM_pages;
    uint64_t g_free_RMM_pages;

    uint64_t wait_migrate_RMMs;
    uint64_t do_migrate_RMMs;
};

extern uint16_t ssd_id_cnt;

#define init_file(fp, path) {fp = fopen(path, "w"); fclose(fp);}
#define open_file(fp, path, type) {fp = fopen(path, type);}
#define my_log(fp, format, ...) { \
    if (fp!=NULL) { \
        fprintf(fp, format, ##__VA_ARGS__); \
        fflush(fp); \
    } \
}
#define debug_log(fp, format, ...) { \
    if (0) { \
        if (fp == NULL) { \
            init_file(fp, "./SSDInfo_all.log"); \
            open_file(fp, "./SSDInfo_all.log", "a"); \
        } \
        fprintf(fp, format, ##__VA_ARGS__); \
        fflush(fp); \
    } \
}

#define my_assert(ssd, expr, format, ...) \
	do { \
		if (! (expr)) { \
            dump_trace(ssd); \
            my_log((ssd)->fp_info, format, ##__VA_ARGS__); \
			assert(expr); \
		} \
	} while (false)

/*#define DEBUG_FEMU_FTL*/
#ifdef DEBUG_FEMU_FTL
#define femu_log(fmt, ...) \
    do { printf("femu: " fmt, ## __VA_ARGS__); } while (0)
#else
#define femu_log(fmt, ...) \
    do { } while (0)
#endif


#endif

struct ppa get_maptbl_ent(struct ssd *ssd, uint64_t lpn, bool if_remote_lpn);
struct rmap_elem get_rmap_ent(struct ssd *ssd, struct ppa *ppa);
bool mapped_ppa(struct ppa *ppa);
bool valid_ppa(struct ssd *ssd, struct ppa *ppa);