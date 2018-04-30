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
#include <math.h>

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

superblock_t supablock;
inode_t inode_table[TOTAL_INODE_NUMBER];
int fd[128];
unsigned char inode_bm[TOTAL_INODE_NUMBER/8];
unsigned char block_bm[TOTAL_DATA_BLOCKS/8];

///////////////////////////////////////////////////////////
//
// Prototypes for all these functions, and the C-style comments,
// come indirectly from /usr/include/fuse.h
//

void set_nth_bit(unsigned char *bitmap, int idx) { bitmap[idx / 8] |= 1 << (idx % 8); }

void clear_nth_bit(unsigned char *bitmap, int idx) { bitmap[idx / 8] &= ~(1 << (idx % 8)); }

int get_nth_bit(unsigned char *bitmap, int idx) { return (bitmap[idx / 8] >> (idx % 8)) & 1; }

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
	int i;
	for(i = 0; i < TOTAL_INODE_NUMBER; i++) {
		if(strcmp(inode_table[i].path, path) == 0){
			return i;
		}
	}
	return -1;
}

int get_next_inode()
{
	int i;
	for(i = 0; i < TOTAL_INODE_NUMBER; i++) {
		if(get_nth_bit(inode_bm, i) == 0) {	return i; }
	}
	return -1;
}

int get_next_block()
{
	int i;
	for(i = 0; i < TOTAL_DATA_BLOCKS; i++) {
		if(get_nth_bit(block_bm, i) == 0) {	return i; }
	}
	return -1;
}

//get the name of file from inode
char* get_name(int i)
{
	int len = strlen(inode_table[i].path);
	char *tmp = inode_table[i].path;
	int find;
	int count = -1;
	for(find = 0; find <= len - 1; find++) {
		if(*(tmp + find) == '/') {
			count = find;
		}
	}
	char *result = malloc(len-count);
	memcpy(result, tmp + count + 1, len-count);
	len = strlen(result);
	*(result + len + 1) = '\0';
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
	fprintf(stderr, "in bb-init\n");
	log_msg("\nsfs_init()\n");
	disk_open((SFS_DATA)->diskfile);
	int in;
	//Data Structure
	for (in = 0; in < TOTAL_INODE_NUMBER; in++) {
		fd[in] = 0;
		inode_table[in].id = in;
		int j;
		for (j = 0; j < 15; j++) {
			inode_table[in].data_blocks[j] = -1;
		}
		memset(&inode_table[in].path, 0, 64);
	}
	memset(inode_bm, 0, TOTAL_INODE_NUMBER/8);
	memset(block_bm, 0, TOTAL_DATA_BLOCKS/8);
	//Pushing everything in diskfile
	//If there is no SFS in the diskfile
	char *buf = (char*) malloc(BLOCK_SIZE);
	if(block_read(0, buf) <= 0) {
		fprintf(stderr, "YAO----INIT New");
		supablock.inodes = TOTAL_INODE_NUMBER;
		supablock.fs_type = 0;
		supablock.data_blocks = TOTAL_DATA_BLOCKS;
		supablock.i_list = 1;
		inode_t *root = &inode_table[0];
		memcpy(&root->path, "/", 1);
		root->st_mode = S_IFDIR;
		root->size = 0;
		root->links = 2;
		root->created = time(NULL);
		root->blocks = 0;
		root->type = TYPE_DIRECTORY;
		set_nth_bit(inode_bm, 0);
		if (block_write(0, &supablock) > 0)
			log_msg("\nInit(): Super Block is written in the file\n");

		if(block_write(1, inode_bm) > 0)
			log_msg("\nInit(): inode bitmap is written in the file\n");

		if(block_write(2, &block_bm) > 0)
			log_msg("\nInit(): block bitmap is written in the file\n");

		int i;
		uint8_t *buffer = malloc(BLOCK_SIZE);
		for(i = 0; i < 128; i++)
		{
			memcpy(buffer, &inode_table[i], sizeof(inode_t));

			if(block_write(i+3, buffer) <= 0) {
				log_msg("\nFailed to write block %d\n", i);
			} else {
				log_msg("\nSucceed to write block %d\n", i);
			}
		}
		free(buffer);
	}
	else {
		fprintf(stderr, "YAO----LOAD old\n");
		uint8_t *buffer = malloc(BLOCK_SIZE*sizeof(uint8_t));
		if(block_read(1, buffer) > 0) {
			memcpy(inode_bm, buffer, TOTAL_INODE_NUMBER/8);
			memset(buffer, 0, BLOCK_SIZE);
		}

		if(block_read(2, buffer) > 0) {
			memcpy(&block_bm, buffer, sizeof(block_bm));
			memset(buffer, 0, BLOCK_SIZE);
		}
		int i;
		inode_t *temp = malloc(sizeof(inode_t));
		for(i = 0; i < TOTAL_INODE_NUMBER; i++) {
			if(block_read(i+3, temp) > 0) {
				inode_table[i].id = temp->id;
				inode_table[i].size = temp->size;
				inode_table[i].blocks = temp->blocks;
				inode_table[i].st_mode = temp->st_mode;
				inode_table[i].created = temp->created;
				int j;
				for(j = 0; j < 15; j++){ inode_table[i].data_blocks[j]=temp->data_blocks[j]; }
				memcpy(&inode_table[i].path, temp->path, 64);
			}
		}
		free(temp);
		free(buffer);
	}

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
	disk_close();
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
		int num = get_next_inode();
		inode_t *tmp = malloc(sizeof(inode_t));
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
			tmp->data_blocks[count] = -1;
			count++;
		}
		memcpy(&inode_table[num], tmp, sizeof(inode_t));
		inode_t *in = &inode_table[num];
		set_nth_bit(inode_bm, num);
		block_write(1, inode_bm);
		uint8_t *buffer = malloc(BLOCK_SIZE);
		memcpy(buffer, &inode_table[i], sizeof(inode_t));
		if(block_write(num+3, buffer) <= 0) { } //retstat = -EEXIST;?
		block_read(num + 3, buffer);
		in = (inode_t*) buffer;		//what does this do??
		free(tmp);
		free(buffer);
	} else{
		retstat = -EEXIST;
	}
	return retstat;
}

/** Remove a file */
int sfs_unlink(const char *path)
{
	int retstat = 0;
	log_msg("sfs_unlink(path=\"%s\")\n", path);
	int i = get_inode_from_path(path);
	if(i == -1) {
		// fprintf(stderr, "Yao---not found");
		retstat = -ENOENT;
		inode_t *tmp = &inode_table[1];
		// fprintf(stderr, "Yao---file name:%s\n",tmp->path);
	}
	else {
		// fprintf(stderr, "Yao---file found at %d \n",i );
		inode_t *tmp = &inode_table[i];
		// fprintf(stderr, "Yao---accessing file \n");
		clear_nth_bit(inode_bm, tmp->id);
		// fprintf(stderr, "Yao---clearing bit \n");
		memset(tmp->path, 0, 64);
		// fprintf(stderr, "Yao---memset path\n");
		int i;
		for(i = 0; i < 15; i++) {
			if(tmp->data_blocks[i] != -1) {
				//clear block bits
				// fprintf(stderr, "Yao---clearing datablock bit%d in %d \n",tmp->data_blocks[i],i);
				clear_nth_bit(block_bm, tmp->data_blocks[i]);
				tmp->data_blocks[i] = -1;
			}
		}
		// fprintf(stderr, "Yao---cleared data blocks \n");
		//Write inode to disk
		uint8_t *buffer = malloc(BLOCK_SIZE);
		memcpy(buffer, &inode_table[i], sizeof(inode_t));
		if(block_write(i+3, buffer) <= 0) { }
		if(block_write(1, inode_bm) < 0) { }
		if(block_write(2, &block_bm) < 0) { }
		free(buffer);
	}
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
	int i = get_inode_from_path(path);
	if(i != -1) {
		if(fd[i] == 0) {
			fd[i] = 1;
		}
		else {
			retstat= -1;
		}
	} else {
		retstat = -1;
	}
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
	int i = get_inode_from_path(path);
	if(i != -1)	{
		if(fd[i] == 1) { fd[i] = 0;	}
		else { retstat= -1;	}
	} else { retstat = -1; }

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

	inode_t *inode = &inode_table[get_inode_from_path(path)];

	int i, blocks_to_read, start_block, bytes_read;
	blocks_to_read = ceil((double)size / (double)BLOCK_SIZE);
	if (inode->blocks <= 0) { return -1; }	//0 should be block_size, will change after I make sure this works
	start_block = inode->data_blocks[0];

	char *read_buf = buf;
	for (i = start_block; i < start_block + blocks_to_read; i++) {
		bytes_read = block_read(i+3, read_buf);
		retstat += bytes_read;
		read_buf += bytes_read;
	}
	//Do I need to handle offsets in read??

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

	char *write_buf;
	int i, j, start_block, size_to_read, size_to_write, size_to_alloc, bytes_written;
	int num = get_inode_from_path(path);
	inode_t *inode = &inode_table[num];

	if (offset == 0) {
		size_to_read = 0;
		size_to_write = size;
	} else {
		size_to_read = inode->size;
		size_to_write = size_to_read + size;
	}
	size_to_alloc = ceil((double)size_to_write / (double)BLOCK_SIZE) * BLOCK_SIZE;

	write_buf = (char *) malloc(size_to_alloc);
	memset(write_buf, 0, size_to_alloc);
	if (offset != 0) {
		retstat = sfs_read(path, write_buf, size_to_read, 0, fi);
		if (retstat < 0) { return retstat; }
	}
	memcpy(write_buf + offset, buf, size);

	inode->size = size_to_write;
	int total_blocks = ceil((double)inode->size / (double) 512);
	int blocks_needed = total_blocks - inode->blocks;

	for (i = inode->blocks; i < total_blocks; i++) {
		inode->data_blocks[i] = get_next_block();
	}

	start_block = inode->data_blocks[0];
	int numFill;
	char *current_write = write_buf;
	retstat = 0;
	for (i = start_block; i < total_blocks + start_block; i++) {
		bytes_written = block_write(i+3, current_write);
		current_write += bytes_written;
		size_to_write -= bytes_written;
		set_nth_bit(block_bm, i);
		retstat += bytes_written;
	}

	inode->blocks = total_blocks;
	free(write_buf);
	set_nth_bit(inode_bm, num);
	block_write(1, inode_bm);
	block_write(2, &block_bm);
	return retstat;
}


/** Create a directory */
int sfs_mkdir(const char *path, mode_t mode)
{
	int retstat = 0;
	log_msg("\nsfs_mkdir(path=\"%s\", mode=0%3o)\n",
			path, mode);
	int i = get_inode_from_path(path);
	if(i == -1) {
		inode_t *tmp = malloc(sizeof(inode_t));
		tmp->id = get_next_inode();
		tmp->type = TYPE_DIRECTORY;
		tmp->st_mode = mode | S_IFDIR;
		memcpy(tmp->path, path, 64);
		tmp->created = time(NULL);
		memcpy(&inode_table[tmp->id], tmp, sizeof(inode_t));
		set_nth_bit(inode_bm, tmp->id);
		if(block_write(tmp->id+3, &inode_table[tmp->id]) <= 0) { }
		if(block_write(1, inode_bm) < 0) { }
		if(block_write(2, &block_bm) < 0) { }
		free(tmp);
	} else {
		retstat = -EEXIST;
	}
	return retstat;
}


/** Remove a directory */
int sfs_rmdir(const char *path)
{
	int retstat = 0;
	log_msg("sfs_rmdir(path=\"%s\")\n",
			path);
	int i = get_inode_from_path(path);
	if(i != -1) {
		int j;
		for(j = 0; j < TOTAL_INODE_NUMBER; j++) {
			if((get_nth_bit(inode_bm, j) != 0) && (j != i)) {
				if(check_parent_dir(path, j) != -1) {
					return -ENOTEMPTY;
				}
			}
		}
		// fprintf(stderr, "Yao---file found at %d \n",i );
		inode_t *tmp = &inode_table[i];
		// fprintf(stderr, "Yao---accessing file \n");
		clear_nth_bit(inode_bm, tmp->id);
		// fprintf(stderr, "Yao---clearing bit \n");
		memset(tmp->path, 0, 64);
		// fprintf(stderr, "Yao---memset path\n");
		int i;
		for(i = 0; i < 15; i++) {
			if(tmp->data_blocks[i] != -1) {
				//clear block bits
				// fprintf(stderr, "Yao---clearing datablock bit%d in %d \n",tmp->data_blocks[i],i);
				clear_nth_bit(block_bm, tmp->data_blocks[i]);
				tmp->data_blocks[i] = -1;
			}
		}
		// fprintf(stderr, "Yao---cleared data blocks \n");
		//Write inode to disk
		uint8_t *buffer = malloc(BLOCK_SIZE);
		memcpy(buffer, &inode_table[i], sizeof(inode_t));
		if(block_write(i+3, buffer) <= 0) { }
		if(block_write(1, inode_bm) < 0) { }
		if(block_write(2, &block_bm) < 0) { }
		free(buffer);
	}
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
	int i = get_inode_from_path(path);
	if(i == -1) {
		log_msg("File not found: %s", path);
		return -ENOENT;
	}
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
	int i;
	for(i = 0; i < TOTAL_INODE_NUMBER; i++) {
		if(strcmp(inode_table[i].path, path) == 0) {
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
			char* file = get_name(i);
			filler(buf, file, statbuf, 0);
			free(file);
			free(statbuf);
		}
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
	log_msg("\nsfs_releasedir(path=\"%s\", fi=0x%08x)\n",
			path, fi);
	int i = get_inode_from_path(path);
	if(i != -1)	{
		if(fd[i] == 1) { fd[i] = 0;	}
		else { retstat= -1;	}
	} else { retstat = -1; }

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
