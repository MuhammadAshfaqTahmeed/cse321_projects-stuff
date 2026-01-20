#define _XOPEN_SOURCE 700
#include <errno.h>
#include <fcntl.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#define BLOCK_SIZE 4096U
#define INODE_SIZE 128U
#define NAME_LEN   28U

#define JOURNAL_BLOCKS 16U

#define SB_BLOCK_NO        0U
#define JOURNAL_START_BLK  1U
#define INODE_BMAP_BLK     (JOURNAL_START_BLK + JOURNAL_BLOCKS)    
#define DATA_BMAP_BLK      (INODE_BMAP_BLK + 1U)                    
#define INODE_TABLE_BLK    (DATA_BMAP_BLK + 1U)                     
#define INODE_TABLE_BLKS   2U                                       
#define DATA_START_BLK     (INODE_TABLE_BLK + INODE_TABLE_BLKS)     

#define DEFAULT_IMAGE "vsfs.img"

struct superblock 
{
    uint32_t magic;
    uint32_t block_size;
    uint32_t total_blocks;
    uint32_t inode_count;

    uint32_t journal_block;
    uint32_t inode_bitmap;
    uint32_t data_bitmap;
    uint32_t inode_start;
    uint32_t data_start;

    uint8_t _pad[128 - 9 * 4];
};

struct inode 
{
    uint16_t type;   
    uint16_t links;
    uint32_t size;
    uint32_t direct[8];
    uint32_t ctime;
    uint32_t mtime;
    uint8_t _pad[128 - (2 + 2 + 4 + 8*4 + 4 + 4)];
};

struct dirent 
{
    uint32_t inode;        
    char name[28];
};

_Static_assert(sizeof(struct superblock) == 128, "superblock must be 128 bytes");
_Static_assert(sizeof(struct inode) == 128, "inode must be 128 bytes");
_Static_assert(sizeof(struct dirent) == 32, "dirent must be 32 bytes");


#define JOURNAL_MAGIC 0x4A524E4CU 
#define REC_DATA      1U
#define REC_COMMIT    2U

struct journal_header 
{
    uint32_t magic;
    uint32_t nbytes_used;
};

struct rec_header 
{
    uint16_t type;
    uint16_t size;
};

_Static_assert(sizeof(struct journal_header) == 8, "journal_header must be 8 bytes");
_Static_assert(sizeof(struct rec_header) == 4, "rec_header must be 4 bytes");


static void die(const char *msg) 
{
    perror(msg);
    exit(1);
}

static void pread_exact(int fd, void *buf, size_t n, off_t off) 
{
    ssize_t r = pread(fd, buf, n, off);
    if (r != (ssize_t)n) die("pread");
}

static void pwrite_exact(int fd, const void *buf, size_t n, off_t off) 
{
    ssize_t r = pwrite(fd, buf, n, off);
    if (r != (ssize_t)n) die("pwrite");
}

static void read_block(int fd, uint32_t blk, void *buf4096) 
{
    pread_exact(fd, buf4096, BLOCK_SIZE, (off_t)blk * (off_t)BLOCK_SIZE);
}

static void write_block(int fd, uint32_t blk, const void *buf4096) 
{
    pwrite_exact(fd, buf4096, BLOCK_SIZE, (off_t)blk * (off_t)BLOCK_SIZE);
}

static int open_image_rw(const char *path) 
{
    int fd = open(path, O_RDWR);
    if (fd < 0) die("open");
    return fd;
}

static int bitmap_test(const uint8_t *bm, uint32_t i) 
{
    return (bm[i / 8U] >> (i % 8U)) & 1U;
}

static void bitmap_set(uint8_t *bm, uint32_t i) 
{
    bm[i / 8U] |= (uint8_t)(1U << (i % 8U));
}

static off_t journal_base_off(void) 
{
    return (off_t)JOURNAL_START_BLK * (off_t)BLOCK_SIZE;
}

static uint32_t journal_capacity_bytes(void) 
{
    return JOURNAL_BLOCKS * BLOCK_SIZE;
}

static void journal_clear_region(int fd) 
{
    uint8_t zero[BLOCK_SIZE];
    memset(zero, 0, sizeof(zero));
    for (uint32_t i = 0; i < JOURNAL_BLOCKS; i++) {
        write_block(fd, JOURNAL_START_BLK + i, zero);
    }
}

static void journal_read_header(int fd, struct journal_header *jh) 
{
    pread_exact(fd, jh, sizeof(*jh), journal_base_off());
}

static void journal_write_header(int fd, const struct journal_header *jh) 
{
    pwrite_exact(fd, jh, sizeof(*jh), journal_base_off());
}

static void journal_init_if_needed(int fd, struct journal_header *jh) 
{
    journal_read_header(fd, jh);
    if (jh->magic != JOURNAL_MAGIC ||
        jh->nbytes_used < sizeof(*jh) ||
        jh->nbytes_used > journal_capacity_bytes()) {

        struct journal_header fresh;
        
        journal_clear_region(fd);
        fresh.magic = JOURNAL_MAGIC;
        fresh.nbytes_used = (uint32_t)sizeof(struct journal_header);
        journal_write_header(fd, &fresh);
        *jh = fresh;
    }
}

static void journal_fail_if_missing(int fd, struct journal_header *jh) 
{
    journal_read_header(fd, jh);
    if (jh->magic != JOURNAL_MAGIC ||
        jh->nbytes_used < sizeof(*jh) ||
        jh->nbytes_used > journal_capacity_bytes()) {
        fprintf(stderr, "ERROR: journal not initialized\n");
        exit(1);
    }
}

static void journal_append_bytes(int fd, struct journal_header *jh, const void *src, uint32_t n) 
{
    if (jh->nbytes_used + n > journal_capacity_bytes()) {
        fprintf(stderr, "ERROR: journal full. Run ./journal install\n");
        exit(1);
    }
    off_t off = journal_base_off() + (off_t)jh->nbytes_used;
    pwrite_exact(fd, src, n, off);
    jh->nbytes_used += n;
    journal_write_header(fd, jh);
}

static void journal_append_data(int fd, struct journal_header *jh, uint32_t block_no, const uint8_t image[BLOCK_SIZE]) 
{
    struct rec_header rh;
    rh.type = (uint16_t)REC_DATA;
    rh.size = (uint16_t)(sizeof(struct rec_header) + sizeof(uint32_t) + BLOCK_SIZE);

    journal_append_bytes(fd, jh, &rh, sizeof(rh));
    journal_append_bytes(fd, jh, &block_no, sizeof(block_no));
    journal_append_bytes(fd, jh, image, BLOCK_SIZE);
}

static void journal_append_commit(int fd, struct journal_header *jh) 
{
    struct rec_header rh;
    rh.type = (uint16_t)REC_COMMIT;
    rh.size = (uint16_t)sizeof(struct rec_header);
    journal_append_bytes(fd, jh, &rh, sizeof(rh));
}

struct logged_image 
{
    uint32_t block_no;
    uint8_t  image[BLOCK_SIZE];
};

static int logged_image_upsert(struct logged_image *arr, uint32_t *count, uint32_t max,
                               uint32_t block_no, const uint8_t image[BLOCK_SIZE]) {
    for (uint32_t i = 0; i < *count; i++) {
        if (arr[i].block_no == block_no) {
            memcpy(arr[i].image, image, BLOCK_SIZE);
            return 1;
        }
    }
    if (*count >= max) return 0;
    arr[*count].block_no = block_no;
    memcpy(arr[*count].image, image, BLOCK_SIZE);
    (*count)++;
    return 1;
}

static const uint8_t *logged_image_find(const struct logged_image *arr, uint32_t count, uint32_t block_no) 
{
    for (uint32_t i = 0; i < count; i++) {
        if (arr[i].block_no == block_no) return arr[i].image;
    }
    return NULL;
}


static void journal_collect_latest_committed(int fd,
                                            const struct journal_header *jh,
                                            struct logged_image *latest,
                                            uint32_t *latest_n,
                                            uint32_t latest_max) {
    *latest_n = 0;

    struct logged_image pending[32];
    uint32_t pending_n = 0;

    uint32_t used = jh->nbytes_used;
    uint32_t pos = (uint32_t)sizeof(struct journal_header);

    while (pos + sizeof(struct rec_header) <= used) {
        struct rec_header rh;
        pread_exact(fd, &rh, sizeof(rh), journal_base_off() + (off_t)pos);

        if (rh.size < sizeof(struct rec_header)) break;
        if (pos + rh.size > used) break; 

        if (rh.type == REC_DATA) {
            uint32_t need = (uint32_t)(sizeof(struct rec_header) + sizeof(uint32_t) + BLOCK_SIZE);
            if (rh.size != need) break;
            if (pending_n >= (uint32_t)(sizeof(pending) / sizeof(pending[0]))) break;

            uint32_t bno;
            pread_exact(fd, &bno, sizeof(bno),
                        journal_base_off() + (off_t)pos + (off_t)sizeof(struct rec_header));
            pread_exact(fd, pending[pending_n].image, BLOCK_SIZE,
                        journal_base_off() + (off_t)pos + (off_t)sizeof(struct rec_header) + (off_t)sizeof(uint32_t));
            pending[pending_n].block_no = bno;
            pending_n++;

        } else if (rh.type == REC_COMMIT) {
            if (rh.size != sizeof(struct rec_header)) break;

            for (uint32_t i = 0; i < pending_n; i++) {
                if (!logged_image_upsert(latest, latest_n, latest_max,
                                         pending[i].block_no, pending[i].image)) {
                    break;
                }
            }
            pending_n = 0;

        } else {
            break; 
        }

        pos += rh.size;
    }
}

//Create() Function
static void cmd_create(const char *name) 
{
    if (!name || name[0] == '\0') {
        fprintf(stderr, "create: missing name\n");
        exit(1);
    }
    if (strlen(name) >= NAME_LEN) {
        fprintf(stderr, "create: name too long (max %u chars)\n", (unsigned)(NAME_LEN - 1));
        exit(1);
    }

    int fd = open_image_rw(DEFAULT_IMAGE);

    struct journal_header jh;
    journal_init_if_needed(fd, &jh);

    
    uint8_t inode_bm[BLOCK_SIZE];
    read_block(fd, INODE_BMAP_BLK, inode_bm);

    
    uint8_t itbl19[BLOCK_SIZE];
    uint8_t itbl20[BLOCK_SIZE];
    read_block(fd, INODE_TABLE_BLK + 0, itbl19);
    read_block(fd, INODE_TABLE_BLK + 1, itbl20);

    struct inode *in19 = (struct inode *)itbl19;
    struct inode *in20 = (struct inode *)itbl20;

    
    if (in19[0].type != 2) {
        fprintf(stderr, "create: root inode not a directory\n");
        close(fd);
        exit(1);
    }

    uint32_t root_dir_block_no = in19[0].direct[0];
    if (root_dir_block_no == 0) {
        fprintf(stderr, "create: root directory has no data block\n");
        close(fd);
        exit(1);
    }

    uint8_t root_dir_img[BLOCK_SIZE];
    read_block(fd, root_dir_block_no, root_dir_img);

    
    if (jh.nbytes_used > (uint32_t)sizeof(struct journal_header)) {
        struct logged_image latest[64];
        uint32_t latest_n = 0;
        journal_collect_latest_committed(fd, &jh, latest, &latest_n, 64);

        const uint8_t *img;

        img = logged_image_find(latest, latest_n, INODE_BMAP_BLK);
        if (img) memcpy(inode_bm, img, BLOCK_SIZE);

        img = logged_image_find(latest, latest_n, INODE_TABLE_BLK + 0);
        if (img) memcpy(itbl19, img, BLOCK_SIZE);

        img = logged_image_find(latest, latest_n, INODE_TABLE_BLK + 1);
        if (img) memcpy(itbl20, img, BLOCK_SIZE);

        
        in19 = (struct inode *)itbl19;
        in20 = (struct inode *)itbl20;

        if (in19[0].type != 2) {
            fprintf(stderr, "create: root inode not a directory\n");
            close(fd);
            exit(1);
        }

        root_dir_block_no = in19[0].direct[0];
        if (root_dir_block_no == 0) {
            fprintf(stderr, "create: root directory has no data block\n");
            close(fd);
            exit(1);
        }

        read_block(fd, root_dir_block_no, root_dir_img);
        img = logged_image_find(latest, latest_n, root_dir_block_no);
        if (img) memcpy(root_dir_img, img, BLOCK_SIZE);
    }

    
    uint32_t new_inum = (uint32_t)-1;
    for (uint32_t i = 1; i < 64U; i++) { 
        if (!bitmap_test(inode_bm, i)) {
            new_inum = i;
            break;
        }
    }
    if (new_inum == (uint32_t)-1) {
        fprintf(stderr, "create: no free inode\n");
        close(fd);
        exit(1);
    }

    
    uint32_t inodes_per_block = BLOCK_SIZE / (uint32_t)sizeof(struct inode); 
    uint32_t inode_block_index = new_inum / inodes_per_block;              
    uint32_t inode_off = new_inum % inodes_per_block;
    if (inode_block_index >= INODE_TABLE_BLKS) {
        fprintf(stderr, "create: inode index out of range\n");
        close(fd);
        exit(1);
    }

    struct inode *target_tbl = (inode_block_index == 0) ? in19 : in20;
    if (target_tbl[inode_off].type != 0) {
        fprintf(stderr, "create: picked inode not free (corrupt?)\n");
        close(fd);
        exit(1);
    }

    
    struct inode *root = &in19[0];
    uint32_t nents = BLOCK_SIZE / (uint32_t)sizeof(struct dirent);
    uint32_t used_entries = root->size / (uint32_t)sizeof(struct dirent);

    if (used_entries < 2) used_entries = 2; 
    if (used_entries >= nents) {
        fprintf(stderr, "create: directory full\n");
        close(fd);
        exit(1);
    }

    struct dirent *ents = (struct dirent *)root_dir_img;

    
    for (uint32_t i = 0; i < used_entries; i++) {
        if (ents[i].inode != 0) {
            if (strncmp(ents[i].name, name, NAME_LEN) == 0) {
                fprintf(stderr, "create: file already exists\n");
                close(fd);
                exit(1);
            }
        }
    }

    uint32_t nmods = 3U + ((inode_block_index == 1U) ? 1U : 0U);
    uint32_t data_rec_size = (uint32_t)(sizeof(struct rec_header) + sizeof(uint32_t) + BLOCK_SIZE);
    uint32_t commit_size = (uint32_t)sizeof(struct rec_header);
    uint32_t txn_bytes = nmods * data_rec_size + commit_size;
    if (jh.nbytes_used + txn_bytes > journal_capacity_bytes()) {
        fprintf(stderr, "ERROR: journal full. Run ./journal install\n");
        close(fd);
        exit(1);
    }

    
    time_t now = time(NULL);

    
    bitmap_set(inode_bm, new_inum);


    struct inode ni;
    memset(&ni, 0, sizeof(ni));
    ni.type  = 1;
    ni.links = 1;
    ni.size  = 0;
    ni.ctime = (uint32_t)now;
    ni.mtime = (uint32_t)now;
    target_tbl[inode_off] = ni;

    
    ents[used_entries].inode = new_inum;
    memset(ents[used_entries].name, 0, NAME_LEN);
    strncpy(ents[used_entries].name, name, NAME_LEN - 1);


    in19[0].size = in19[0].size + (uint32_t)sizeof(struct dirent);
    in19[0].mtime = (uint32_t)now;

    journal_append_data(fd, &jh, INODE_BMAP_BLK, inode_bm);


    journal_append_data(fd, &jh, INODE_TABLE_BLK + 0, itbl19);


    if (inode_block_index == 1) {
        journal_append_data(fd, &jh, INODE_TABLE_BLK + 1, itbl20);
    }


    journal_append_data(fd, &jh, root_dir_block_no, root_dir_img);

    journal_append_commit(fd, &jh);

    close(fd);
    printf("Logged creation of '%s' to journal.\n",name);
}

// Install() function
static void cmd_install(void) 
{
    int fd = open_image_rw(DEFAULT_IMAGE);

    struct journal_header jh;
    journal_fail_if_missing(fd, &jh);

    uint32_t used = jh.nbytes_used;
    uint32_t pos = (uint32_t)sizeof(struct journal_header);

    int commits = 0; 

    struct pending {
        uint32_t block_no;
        uint8_t image[BLOCK_SIZE];
    } pending[64];
    uint32_t pending_n = 0;

    while (pos + sizeof(struct rec_header) <= used) {
        struct rec_header rh;
        pread_exact(fd, &rh, sizeof(rh), journal_base_off() + (off_t)pos);

        if (rh.size < sizeof(struct rec_header)) break;
        if (pos + rh.size > used) break; 

        if (rh.type == REC_DATA) {
            uint32_t need = (uint32_t)(sizeof(struct rec_header) + sizeof(uint32_t) + BLOCK_SIZE);
            if (rh.size != need) break;

            if (pending_n >= 64) {
                fprintf(stderr, "install: too many records in one txn\n");
                close(fd);
                exit(1);
            }

            uint32_t bno;
            pread_exact(fd, &bno, sizeof(bno),
                        journal_base_off() + (off_t)pos + (off_t)sizeof(struct rec_header));
            pread_exact(fd, pending[pending_n].image, BLOCK_SIZE,
                        journal_base_off() + (off_t)pos + (off_t)sizeof(struct rec_header) + (off_t)sizeof(uint32_t));
            pending[pending_n].block_no = bno;
            pending_n++;
        } else if (rh.type == REC_COMMIT) {
            if (rh.size != sizeof(struct rec_header)) break;

            
            for (uint32_t i = 0; i < pending_n; i++) 
            {
                write_block(fd, pending[i].block_no, pending[i].image);
            }
            pending_n = 0;

            commits++;
        } else 
        {
            break; 
        }

        pos += rh.size;
    }

    
    journal_clear_region(fd);
    struct journal_header cleared;
    cleared.magic = JOURNAL_MAGIC;
    cleared.nbytes_used = (uint32_t)sizeof(struct journal_header);
    journal_write_header(fd, &cleared);

    close(fd);
    printf("Installed %d commited transactions from journal.\n", commits);
}

//main() function (calls create() and insta())
int main(int argc, char *argv[]) 
{
    if (argc < 2) 
    {
        fprintf(stderr, "Usage:\n");
        fprintf(stderr, "  ./journal create <name>\n");
        fprintf(stderr, "  ./journal install\n");
        return 1;
    }

    if (strcmp(argv[1], "create") == 0) 
    {
        if (argc != 3) 
        {
            fprintf(stderr, "Usage: ./journal create <name>\n");
            return 1;
        }
        cmd_create(argv[2]);
        return 0;
    }

    if (strcmp(argv[1], "install") == 0) 
    {
        cmd_install();
        return 0;
    }

    fprintf(stderr, "Unknown command: %s\n", argv[1]);
    return 1;
}
