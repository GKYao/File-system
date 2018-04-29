/*
  Simple File System
  This code is derived from function prototypes found /usr/include/fuse/fuse.h
  Copyright (C) 2001-2007  Miklos Szeredi <miklos@szeredi.hu>
  His code is licensed under the LGPLv2.
 */

#include "params.h"
#include "block.h"

#include <ctype.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <fuse.h>
#include <libgen.h>
#include <limits.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>

#ifdef HAVE_SYS_XATTR_H
#include <sys/xattr.h>
#endif
#define TOTAL_BLOCKS 1024 //To be changed later
#define TOTAL_INODE_NUMBER 128
#define TOTAL_DATA_BLOCKS (TOTAL_BLOCKS - TOTAL_INODE_NUMBER - 1)
#define BLOCK_SIZE 512
#define TYPE_DIRECTORY 0
#define TYPE_FILE 1
#define TYPE_LINK 2  
#include "log.h"
//////////////////////////////////////////////////////////USR///////////////////////////////////////////////////////


typedef struct superblock {
	int inodes;
	int fs_type;
	int data_blocks;
	int i_list;
} superblock_t;

typedef struct inode {
	//size equal to 512->one block
	int id;
	int size;
	int type;
	int links;
	int blocks;
	mode_t st_mode;
	unsigned char path[64];
	unsigned int data_blocks[15];
	time_t created, modified;
	char unusedspace[340];
} inode_t;

typedef struct file_descriptor {
	int id;
	int inode_id;
} fd_t;

typedef struct byte_fields {
	unsigned int bit0:1;
	unsigned int bit1:1;
	unsigned int bit2:1;
	unsigned int bit3:1;
	unsigned int bit4:1;
	unsigned int bit5:1;
	unsigned int bit6:1;
	unsigned int bit7:1;
} byte_fields;

typedef union byte_t {
	unsigned int byte;
	byte_fields bits;
} byte_t;

superblock_t supablock;
inode_t inode_table[TOTAL_INODE_NUMBER];
fd_t fd_table[TOTAL_INODE_NUMBER];
unsigned char inode_bm[TOTAL_INODE_NUMBER/8];
unsigned char block_bm[TOTAL_DATA_BLOCKS/8];

///////////////////////////////////////////////////////////
//
// Prototypes for all these functions, and the C-style comments,
// come indirectly from /usr/include/fuse.h
//

void set_nth_bit(unsigned char *bitmap, int idx) { bitmap[idx / 8] |= 1 << (idx % 8); }

void clear_nth_bit(unsigned char *bitmap, int idx) { bitmap[idx / 8] &= ~(1 << (idx % 8)); }

void set_inode_bit(int index, int bit)
{
	if(bit == 1){
		set_nth_bit(inode_bm, index);
	}
	else
		clear_nth_bit(inode_bm, index);
}

int get_inode_from_path(const char* path)
{
	int i = 0;
	while(i<TOTAL_INODE_NUMBER)
	{
		if(strcmp(inode_table[i].path, path) == 0){
			return i;
		}
		i++;
	}
	return -1;
}

int get_bit(unsigned char dataByte, int bit)
{
	if (bit > 7) return -1;
	byte_t thisByte;
	thisByte.byte = dataByte;
	int thisBit;
	switch (bit) {
	case 0: thisBit = thisByte.bits.bit0; break;
	case 1: thisBit = thisByte.bits.bit1; break;
	case 2: thisBit = thisByte.bits.bit2; break;
	case 3: thisBit = thisByte.bits.bit3; break;
	case 4: thisBit = thisByte.bits.bit4; break;
	case 5: thisBit = thisByte.bits.bit5; break;
	case 6: thisBit = thisByte.bits.bit6; break;
	case 7: thisBit = thisByte.bits.bit7; break;
	default : thisBit = -1; break;
	}

	return thisBit;
}

int get_next_free(unsigned char bitmap[])
{
	int i, j, num;
	num = 0;
	for (i = 0; i < sizeof(bitmap); i++) {
		unsigned char bmByte = bitmap[i];
		for (j = 0; j < 8; j++) {
			if (get_bit(bmByte, j) == 0) return num;
			num++;
		}
		num++;
	}
	return -1;
}

//get the name of file from inode
char* get_name(int i)
{
	int len = strlen(inode_table[i].path);
	char *tmp =inode_table[i].path;
	int find=0;
	int count=-1;
	while(find<=len-1) {
		if(*(tmp+find) == '/') {
			count=find;
		}
		find++;
	}
	char *result = malloc(len-count);
	memcpy(result, tmp+count+1, len-count);
	len=strlen(result);
	*(result+len+1)='\0';
	return result;
}

//check if given path is the root of this inode
int check_parent_dir(const char* path,int i)
{
	char *tmp = malloc(64*sizeof(char));
	int len = strlen(inode_table[i].path);
	memcpy(tmp, inode_table[i].path, len);
	*(tmp+len) = '\0';
	int offset = 0;
	int count = -1;
	while(offset <= len-1)  {
		if(*(tmp+offset) == '/') {
			count=offset;
		}
		offset++;
	}
	if(count != -1) {
		if(count == 0) {
			tmp="/";
		} else {
			*(tmp+count) = '\0';
		}
	}
	if(strcmp(tmp, path) == 0) {
		return 0;
	}
	return -1;
}

/**
 * Initialize filesystem
 *
 * The return value will passed in the private_data field of
 * fuse_context to all file operations and as a parameter to the
 * destroy() method.
 *
 * Introduced in version 2.3
 * Changed in version 2.6
 */
void *sfs_init(struct fuse_conn_info *conn)
{
	fprintf(stderr, "YAO-----Init\n");
	fprintf(stderr, "in bb-init\n");
	log_msg("\nsfs_init()\n");
	disk_open((SFS_DATA)->diskfile);
	int in;
	//Data Structure
	for (in = 0; in < TOTAL_INODE_NUMBER; in++) {
		memset(inode_table[in], 0, sizeof(inode_t));
		int j;
		for (j = 0; j < 15; j++) {
			inode_table[in].data_blocks[j] = -1;
		}
	}
	memset(inode_bm, 0, TOTAL_INODE_NUMBER/8);
	memset(block_bm, 0, TOTAL_DATA_BLOCKS/8);

	//Pushing everything in diskfile
	//If there us no SFS in the diskfile

	char *buf = (char*) malloc(BLOCK_SIZE);

	// initialize superblock etc here in file
	supablock.inodes = TOTAL_INODE_NUMBER;
	supablock.fs_type = 0;
	supablock.data_blocks = TOTAL_DATA_BLOCKS;
	supablock.i_list = 1;

	//init the root i-node here
	inode_t *root = &inode_table[0];
	memcpy(&root->path, "/", 1);
	root->st_mode = S_IFDIR;
	root->size = 0;
	root->links = 2;
	root->created = time(NULL);
	root->blocks = 0;
	root->type = TYPE_DIRECTORY;

	set_inode_bit(0, 1); // set the bit map for root

	if (block_write(0, &supablock) > 0)
		log_msg("\nInit(): Super Block is written in the file\n");

	if(block_write(1, &inode_bm) > 0)
		log_msg("\nInit(): inode bitmap is written in the file\n");

	if(block_write(2, &block_bm) > 0)
		log_msg("\nInit(): block bitmap is written in the file\n");

	int i;
	uint8_t *buffer = malloc(BLOCK_SIZE);
	for(i = 0; i < 128; i++) {
		memcpy(buffer, &inode_table[i], sizeof(struct inode_t));

		if(block_write(i+3, buffer) <= 0) {
			log_msg("\nFailed to write block %d\n", i);
		} else {
			log_msg("\nSucceed to write block %d\n", i);
		}
	}
	free(buffer);

	log_conn(conn);
	log_fuse_context(fuse_get_context());

	return SFS_DATA;
}

/**
 * Clean up filesystem
 *
 * Called on filesystem exit.
 *
 * Introduced in version 2.3
 */
void sfs_destroy(void *userdata)
{
	log_msg("\nsfs_destroy(userdata=0x%08x)\n", userdata);
}

/** Get file attributes.
 *
 * Similar to stat().  The 'st_dev' and 'st_blksize' fields are
 * ignored.  The 'st_ino' field is ignored except if the 'use_ino'
 * mount option is given.
 */
int sfs_getattr(const char *path, struct stat *statbuf)
{
	int retstat = 0;
	char fpath[PATH_MAX];
	log_msg("\nsfs_getattr(path=\"%s\", statbuf=0x%08x)\n",
			path, statbuf);
	// fprintf(stderr, "YAO Attr-----start\n");
	int inode = get_inode_from_path(path);
	if (inode != -1) {
		inode_t *tmp = &inode_table[inode];
		// fprintf(stderr, "YAO Attr-----found:%s\n",tmp->path);
		statbuf->st_mode = tmp->st_mode;
		statbuf->st_ctime = tmp->created;
		statbuf->st_size = tmp->size;
		statbuf->st_blocks = tmp->blocks;
	} else {
		log_msg("\n\nInode not found for path: %s\n\n", path);
		retstat = -ENOENT;
	}
	log_stat(statbuf);
	// fprintf(stderr, "YAO Attr-----finish\n");
	return retstat;
}


/**
 * Create and open a file
 *
 * If the file does not exist, first create it with the specified
 * mode, and then open it.
 *
 * If this method is not implemented or under Linux kernel
 * versions earlier than 2.6.15, the mknod() and open() methods
 * will be called instead.
 *
 * Introduced in version 2.5
 */
int sfs_create(const char *path, mode_t mode, struct fuse_file_info *fi)
{
	int retstat = 0;
	log_msg("\nsfs_create(path=\"%s\", mode=0%03o, fi=0x%08x)\n",
			path, mode, fi);
	int i = get_inode_from_path(path);
	if(i == -1) {
		int num = get_next_free(inode_bm);
		struct inode_t *tmp = malloc(sizeof(struct inode_t));
		tmp->id = num;
		tmp->size = 0;
		tmp->blocks = 0;
		tmp->st_mode = mode;
		tmp->created = time(NULL);
		memcpy(tmp->path, path, 64);
		if(S_ISDIR(mode)) {
			tmp->type = TYPE_DIRECTORY;
		}
		else{
			tmp->type = TYPE_FILE;
		}
		int count = 0;
		while(count != 15){
			tmp->data_blocks[count]=-1;
			count++;
		}
		memcpy(&inode_table[num], tmp, sizeof(struct inode_t));
		struct inode_t *in = &inode_table[num];
		set_nth_bit(inode_bm, num);
		free(tmp);
		block_write(1, &inode_bm);
		uint8_t *buffer = malloc(BLOCK_SIZE);
		memcpy(buffer, &inode_table[i], sizeof(struct inode_t));
		if(block_write(i+3, buffer) <= 0) retstat = -EEXIST;
		free(buffer);
	} else{
		retstat = -EEXIST;
	}
	struct inode_t *tmp= &inode_table[1];
	return retstat;
}

/** Remove a file */
int sfs_unlink(const char *path)
{
	int retstat = 0;
	log_msg("sfs_unlink(path=\"%s\")\n", path);


	return retstat;
}

/** File open operation
 *
 * No creation, or truncation flags (O_CREAT, O_EXCL, O_TRUNC)
 * will be passed to open().  Open should check if the operation
 * is permitted for the given flags.  Optionally open may also
 * return an arbitrary filehandle in the fuse_file_info structure,
 * which will be passed to all file operations.
 *
 * Changed in version 2.2
 */
int sfs_open(const char *path, struct fuse_file_info *fi)
{
	int retstat = 0;
	log_msg("\nsfs_open(path\"%s\", fi=0x%08x)\n",
			path, fi);
	return retstat;
}

/** Release an open file
 *
 * Release is called when there are no more references to an open
 * file: all file descriptors are closed and all memory mappings
 * are unmapped.
 *
 * For every open() call there will be exactly one release() call
 * with the same flags and file descriptor.  It is possible to
 * have a file opened more than once, in which case only the last
 * release will mean, that no more reads/writes will happen on the
 * file.  The return value of release is ignored.
 *
 * Changed in version 2.2
 */
int sfs_release(const char *path, struct fuse_file_info *fi)
{
	int retstat = 0;
	log_msg("\nsfs_release(path=\"%s\", fi=0x%08x)\n",
			path, fi);


	return retstat;
}

/** Read data from an open file
 *
 * Read should return exactly the number of bytes requested except
 * on EOF or error, otherwise the rest of the data will be
 * substituted with zeroes.  An exception to this is when the
 * 'direct_io' mount option is specified, in which case the return
 * value of the read system call will reflect the return value of
 * this operation.
 *
 * Changed in version 2.2
 */
int sfs_read(const char *path, char *buf, size_t size, off_t offset, struct fuse_file_info *fi)
{
	int retstat = 0;
	log_msg("\nsfs_read(path=\"%s\", buf=0x%08x, size=%d, offset=%lld, fi=0x%08x)\n",
			path, buf, size, offset, fi);
	inode_t *inode = inode_table[get_inode_from_path(path)];
	if (inode->blocks <= 0) return -1;
	int i, blocks_to_read, start_block;
	blocks_to_read = (size + offset)/BLOCK_SIZE;
	start_block = inode->data_blocks[0];
	if (blocks_to_read > 1) {
		char *read_block = malloc(BLOCK_SIZE);
		memset(read_block, 0, strlen(read_block));
		retstat += block_read(start_block, read_block);
		read_block += offset;
		memcpy(buf, read_block, (strlen(read_block) - offset));
		for (i = start_block+1; i < blocks_to_read+start_block-1; i++) {
			retstat += block_read(i, read_block);
			memcpy(buf, read_block, strlen(read_block));
		}
		retstat += block_read(i, read_block);
		memcpy(buf, read_block, size%BLOCK_SIZE);
		free(read_block);
	}
	else {
		retstat += (block_read(start_block, buf) - offset);
		buf += offset;
	}
	return retstat;
}

/** Write data to an open file
 *
 * Write should return exactly the number of bytes requested
 * except on error.  An exception to this is when the 'direct_io'
 * mount option is specified (see read operation).
 *
 * Changed in version 2.2
 */
int sfs_write(const char *path, const char *buf, size_t size, off_t offset,
		struct fuse_file_info *fi)
{
	int retstat = 0;
	log_msg("\nsfs_write(path=\"%s\", buf=0x%08x, size=%d, offset=%lld, fi=0x%08x)\n",
			path, buf, size, offset, fi);
	inode_t *inode = inode_table[get_inode_from_path(path)];
	int i, j, start_block, size_to_read;
	size_to_read = inode->size;
	char *total_write = malloc(size_to_read + size + offset + 1);
	memset(total_write, 0, strlen(total_write));
	if (inode->blocks != 0) {
		retstat = sfs_read(path, total_write, size, 0, fi);
		if (retstat < 0) return retstat;
		memcpy(total_write, buf + (inode->size + offset), strlen(buf));
		if (inode->size < (strlen(buf) + offset)) {
			inode->size += (strlen(buf) + offset);
			(total_write + inode->size) = '\0';
		}
	}
	int total_blocks = inode->size / 512;
	int blocks_needed = total_blocks - inode->blocks;
	for (i = inode->blocks; i < total_blocks; i++) {
		inode->data_blocks[i] = get_next_free(block_bm);
	}
	start_block = inode->data_blocks[0];
	char *write_buf = (char *) malloc(BLOCK_SIZE);
	for (i = start_block; i < total_blocks + start_block; i++) {
		memset(write_buf, 0, BLOCK_SIZE);
		memcpy(write_buf, buf, strlen(write_buf));
		int cur;
		cur = block_write(i, write_buf);
		retstat += cur;
		write_buf += cur;
	}
	inode->blocks = total_blocks;
	free(total_write);
	return retstat;
}


/** Create a directory */
int sfs_mkdir(const char *path, mode_t mode)
{
	int retstat = 0;
	log_msg("\nsfs_mkdir(path=\"%s\", mode=0%3o)\n",
			path, mode);


	return retstat;
}


/** Remove a directory */
int sfs_rmdir(const char *path)
{
	int retstat = 0;
	log_msg("sfs_rmdir(path=\"%s\")\n",
			path);


	return retstat;
}


/** Open directory
 *
 * This method should check if the open operation is permitted for
 * this  directory
 *
 * Introduced in version 2.3
 */
int sfs_opendir(const char *path, struct fuse_file_info *fi)
{
	int retstat = 0;
	log_msg("\nsfs_opendir(path=\"%s\", fi=0x%08x)\n",
			path, fi);


	return retstat;
}

/** Read directory
 *
 * This supersedes the old getdir() interface.  New applications
 * should use this.
 *
 * The filesystem may choose between two modes of operation:
 *
 * 1) The readdir implementation ignores the offset parameter, and
 * passes zero to the filler function's offset.  The filler
 * function will not return '1' (unless an error happens), so the
 * whole directory is read in a single readdir operation.  This
 * works just like the old getdir() method.
 *
 * 2) The readdir implementation keeps track of the offsets of the
 * directory entries.  It uses the offset parameter and always
 * passes non-zero offset to the filler function.  When the buffer
 * is full (or an error happens) the filler function will return
 * '1'.
 *
 * Introduced in version 2.3
 */
int sfs_readdir(const char *path, void *buf, fuse_fill_dir_t filler, off_t offset,
		struct fuse_file_info *fi)
{
	int retstat = 0;
	// fprintf(stderr, "YAO readdir-----start\n");
	filler(buf, ".", NULL, 0);
	filler(buf, "..", NULL, 0);
	int i = 0;
	while(i < TOTAL_INODE_NUMBER) {
		if(strcmp(inode_table[i].path, path)==0) {
			i++;
			continue;
		}
		if(check_parent_dir(path, i) != -1) {
			// fprintf(stderr, "YAO readdir-----found:%d\n",i);
			struct stat *statbuf = malloc(sizeof(struct stat));
			inode_t *tmp = &inode_table[i];
			statbuf->st_mode = tmp->st_mode;
			statbuf->st_ctime = tmp->created;
			statbuf->st_size = tmp->size;
			statbuf->st_blocks = tmp->blocks;
			char* file =get_name(i);
			filler(buf,file,statbuf,0);
			free(file);
			free(statbuf);
		}
		i++;
	}
	// fprintf(stderr, "YAO readdir-----finish\n");

	return retstat;
}

/** Release directory
 *
 * Introduced in version 2.3
 */
int sfs_releasedir(const char *path, struct fuse_file_info *fi)
{
	int retstat = 0;


	return retstat;
}

struct fuse_operations sfs_oper = {
		.init = sfs_init,
		.destroy = sfs_destroy,

		.getattr = sfs_getattr,
		.create = sfs_create,
		.unlink = sfs_unlink,
		.open = sfs_open,
		.release = sfs_release,
		.read = sfs_read,
		.write = sfs_write,

		.rmdir = sfs_rmdir,
		.mkdir = sfs_mkdir,

		.opendir = sfs_opendir,
		.readdir = sfs_readdir,
		.releasedir = sfs_releasedir
};

void sfs_usage()
{
	fprintf(stderr, "usage:  sfs [FUSE and mount options] diskFile mountPoint\n");
	abort();
}

int main(int argc, char *argv[])
{
	int fuse_stat;
	struct sfs_state *sfs_data;

	// sanity checking on the command line
	if ((argc < 3) || (argv[argc-2][0] == '-') || (argv[argc-1][0] == '-'))
		sfs_usage();

	sfs_data = malloc(sizeof(struct sfs_state));
	if (sfs_data == NULL) {
		perror("main calloc");
		abort();
	}

	// Pull the diskfile and save it in internal data
	sfs_data->diskfile = argv[argc-2];
	argv[argc-2] = argv[argc-1];
	argv[argc-1] = NULL;
	argc--;

	sfs_data->logfile = log_open();

	// turn over control to fuse
	fprintf(stderr, "about to call fuse_main, %s \n", sfs_data->diskfile);
	fuse_stat = fuse_main(argc, argv, &sfs_oper, sfs_data);
	fprintf(stderr, "fuse_main returned %d\n", fuse_stat);

	return fuse_stat;
}
