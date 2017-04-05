/*
 * sheepdog engine
 *
 * IO engine using a distributed object storage "Sheepdog".
 *
 * https://github.com/sheepdog/sheepdog
 */
#include "../fio.h"
#include "../optgroup.h"

#include "sheepdog.h"

struct sheepdog_data {
	struct sheepdog_access_info *ai;
};

struct sheepdog_options {
        void *pad;
        char *target;
};

static struct fio_option options[] = {
        {
                .name           = "target",
                .lname          = "sheepdog target",
                .type           = FIO_OPT_STR_STORE,
                .help           = "String of sheepdog access information including protocol, hostname, port and vdiname",
                .off1           = offsetof(struct sheepdog_options, target),
                .category       = FIO_OPT_C_ENGINE,
                .group          = FIO_OPT_G_SHEEPDOG,
        },
        {
                .name = NULL,
        },
};

static int fio_sheepdog_queue(struct thread_data *td, struct io_u *io_u)
{
        struct sheepdog_data *sd = td->io_ops_data;
        int ret = 0;

        dprint(FD_IO, "%s op %s\n", __FUNCTION__, io_ddir_name(io_u->ddir));

        fio_ro_check(td, io_u);

        if (io_u->ddir == DDIR_READ)
                ret = sd_io(sd->ai, 0, io_u->xfer_buf, io_u->xfer_buflen, io_u->offset);
        else if (io_u->ddir == DDIR_WRITE)
                ret = sd_io(sd->ai, 1, io_u->xfer_buf, io_u->xfer_buflen, io_u->offset);
        else {
                log_err("unsupported operation.\n");
                return -EINVAL;
        }

	if (ret) {
		log_err("sheepdog queue failed.\n");
		io_u->error = ret;
		td_verror(td, io_u->error, "xfer");
		return FIO_Q_COMPLETED;
	}
	return FIO_Q_COMPLETED;
}

static int fio_sheepdog_init(struct thread_data *td)
{
	struct sheepdog_data *sd;
	struct sheepdog_options *opt = td->eo;
	int ret = 0;
        
	if (td->io_ops_data)
		return 0;

	sd = calloc(1, sizeof(*sd));
	if (!sd) {
		log_err("malloc failed.\n");
		return -ENOMEM;
	} 

	sd->ai = malloc(sizeof(struct sheepdog_access_info));
	if (!sd->ai) {
		log_err("malloc failed.\n");
		free(sd);
		return -ENOMEM;
	}

	INIT_LIST_HEAD(&sd->ai->fd_list_head);
	pthread_rwlock_init(&sd->ai->fd_list_lock, NULL);
	pthread_rwlock_init(&sd->ai->inode_lock, NULL);
	pthread_mutex_init(&sd->ai->inode_version_mutex, NULL);

	ret = sd_open(sd->ai, opt->target, 0);

	if (ret) {
		sd_close(sd->ai);
		free(sd->ai);
		free(sd);
		return ret;
	} else {
		log_info("Job No.%d, connected to target: %s\n", td->thread_number, opt->target);
	}

	td->io_ops_data = sd;

	sd_close(sd->ai);

	return ret;
}

static void fio_sheepdog_cleanup(struct thread_data *td)
{
	struct sheepdog_data *sd = td->io_ops_data;
	struct sheepdog_fd_list *p, *next;

        list_for_each_entry_safe(p, next, &sd->ai->fd_list_head, list) {
                close(p->fd);
                list_del(&p->list);
                free(p);
        }

        pthread_rwlock_destroy(&sd->ai->fd_list_lock);
        pthread_rwlock_destroy(&sd->ai->inode_lock);


	sd_close(sd->ai);

	free(sd->ai);
	free(sd);

	td->io_ops_data = NULL;
}

#define READFILE_BLOCK_SIZE 4194304
static int fio_sheepdog_open(struct thread_data *td, struct fio_file *f)
{

	struct sheepdog_data *sd = td->io_ops_data;
        unsigned long long left, offset;
        unsigned int bs;
        char *b;
        int ret;

	if (td_read(td)) {
		bs = READFILE_BLOCK_SIZE;
		b = malloc(bs);
		left = f->real_file_size;
		offset = 0;		

		while(left && !td->terminate) {
			if(bs > left)
				bs = left;
			fill_io_buffer(td, b, bs, bs);
			ret = sd_io(sd->ai, 1, b, bs, offset);
			if(ret)
				return ret;
			offset += bs;
			left -= bs;
		}
		free(b);
		fio_time_init();
	}

	return 0;
}

static int fio_sheepdog_close(struct thread_data *td, struct fio_file *f)
{
	return 0;
}

struct ioengine_ops ioengine = {
	.name		= "sheepdog",
	.version	= FIO_IOOPS_VERSION,
	.init		= fio_sheepdog_init,
	.queue		= fio_sheepdog_queue,
	.cleanup	= fio_sheepdog_cleanup,
	.open_file	= fio_sheepdog_open,
	.close_file	= fio_sheepdog_close,
	.options                = options,
        .option_struct_size     = sizeof(struct sheepdog_options),
	.flags = FIO_SYNCIO | FIO_DISKLESSIO,
};

static void fio_init fio_sheepdog_register(void)
{
        register_ioengine(&ioengine);
}

static void fio_exit fio_sheepdog_unregister(void)
{
        unregister_ioengine(&ioengine);
}
