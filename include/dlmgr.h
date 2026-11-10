#pragma once
#include <stdint.h>
#include <stddef.h>

#define DL_PORT      5837
#define DL_VERSION   "0.1.0"
#define DL_TITLE_ID  "DLMG00001"

#define MAX_SEG 8
#define MAX_Q   16
#define MAX_DL  512

enum { ST_QUEUED = 0, ST_ACTIVE, ST_PAUSED, ST_DONE, ST_ERROR };

typedef struct {
    int segments;          /* parallel connections per download (1..8)  */
    int limit_kbps;        /* per-download speed cap, 0 = unlimited     */
    int retries;           /* per-segment retries before giving up      */
    int auto_install_app;  /* PS5: add the Media-tab shortcut at start  */
    int notify;            /* 0 off, 1 queue finished, 2 every download */
    char dir[512];         /* default download folder                   */
} Settings;

/* ---- platform layer (platform_ps5.c) ---- */
void plat_init(void);
void plat_notify(const char *fmt, ...);
int  plat_local_ip(char *buf, size_t n);          /* 0 on success */
int  plat_install_app(void);
void plat_kill_previous_instance(void);

/* ---- engine ---- */
const char *dl_base(void);                         /* data root, /data/DownloadMGR on PS5 */
void  log_msg(const char *fmt, ...);
void  eng_init(void);
void  eng_shutdown(void);
int   eng_item_action(int id, const char *act, const char *arg);
int   eng_add(const char *url, const char *name, const char *dir, int qid, char *err, size_t errn, int *dup);   /* dup (may be NULL): 1 = already listed (not added), 2 = downloaded before (added) */
int   eng_queue_create(const char *name);
int   eng_queue_update(int qid, const char *name, int max, int running, int limit_kbps, int chain);   /* -1 = leave */
int   eng_queue_schedule(int qid, int on, int start_min, int stop_min, int tz);
int   eng_queue_delete(int qid);
void  eng_settings_get(Settings *out);
int   eng_settings_set(const Settings *in, char *err, size_t errn);
char *eng_state_json(void);                        /* malloc'd, caller frees */
char *eng_ls_json(const char *path, char *err, size_t errn);   /* folder browser: sub folders of an allowed folder, or the roots when path is empty */
int   eng_mkdir(const char *parent, const char *name, char *out, size_t on, char *err, size_t en);
char *eng_scan(const char *url, char *err, size_t errn);   /* link grabber: malloc'd JSON or NULL */
const char *eng_token(void);
