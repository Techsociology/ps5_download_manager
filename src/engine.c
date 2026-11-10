/*
 * Download engine: queues, segmented (multi-connection) HTTP(S) downloads with resume,
 * persistence, watch-folder ingestion. Pure POSIX + libcurl, no PS5 calls (see platform_*.c).
 */
#define _GNU_SOURCE
#include <ctype.h>
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <dirent.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <time.h>
#include <unistd.h>
#include <curl/curl.h>
#include "dlmgr.h"
#include "assets_cacert_pem.h"

#define SEG_MIN_BYTES (512 * 1024)

typedef struct { int64_t start, end, pos; } Seg;   /* end == -1: unknown length (stream) */

typedef struct {
    int used, id, qid, state; long ord;
    char url[2048], name[256], dir[512], path[1024], err[160];
    int64_t total;               /* -1 unknown */
    int resumable, nseg;
    Seg seg[MAX_SEG];
    time_t added;
    /* runtime only */
    volatile int stop, remove_req, running, stop_to;
    double speed; int64_t last_done;
} Download;

typedef struct { int used, id, running, max; char name[64];
    int sched_on, start_min, stop_min, tz;   /* timer: auto start/stop, minutes since midnight (client local time), tz = JS getTimezoneOffset */
    int limit_kbps;                          /* total speed cap for the whole queue, 0 = unlimited */
    int chain;                               /* queue id to start when this queue finishes, 0 = none */
    int fin_pending, was_busy;               /* runtime: something finished since the queue last went idle / was busy last tick */
    int last_in; } Queue;     /* last_in: runtime only, -1 = not evaluated yet */

static pthread_mutex_t L = PTHREAD_MUTEX_INITIALIZER;
static Download dls[MAX_DL];
static Queue qs[MAX_Q];
static Settings S;
static int next_id = 1, next_qid = 2;
static long next_ord = 1;
static volatile int quit_flag, dirty;
static pthread_t sched_thr;
static char token[40], base[256];

/* ------------------------------------------------------------------ utils */
const char *dl_base(void) {
    if (!base[0]) snprintf(base, sizeof base, "%s", "/data/DownloadMGR");
    return base;
}
const char *eng_token(void) { return token; }

void log_msg(const char *fmt, ...) {
    va_list a; char buf[1024]; va_start(a, fmt); vsnprintf(buf, sizeof buf, fmt, a); va_end(a);
    fputs(buf, stderr);
}

static void ssafe(char *dst, size_t n, const char *src) { snprintf(dst, n, "%s", src ? src : ""); }

static int mkdir_p(const char *path) {
    char t[1024]; ssafe(t, sizeof t, path);
    for (char *p = t + 1; *p; p++)
        if (*p == '/') { *p = 0; if (mkdir(t, 0755) && errno != EEXIST) return -1; *p = '/'; }
    return (mkdir(t, 0755) && errno != EEXIST) ? -1 : 0;
}

static int eng_dir_allowed(const char *d) {
    if (!d || d[0] != '/' || strlen(d) > 500 || strstr(d, "..") || strstr(d, "//")) return 0;
    for (const char *p = d; *p; p++) if ((unsigned char)*p < 32) return 0;
    size_t bl = strlen(dl_base());
    if (!strncmp(d, dl_base(), bl) && (d[bl] == '/' || !d[bl])) return 1;
    /* USB / extended drives: only the DownloadMGR folder on the drive, e.g. /mnt/usb0/DownloadMGR/... */
    for (int k = 0; k < 2; k++) {
        const char *pre = k ? "/mnt/ext" : "/mnt/usb";
        if (!strncmp(d, pre, 8) && isdigit((unsigned char)d[8]) && !strncmp(d + 9, "/DownloadMGR", 12) && (d[21] == '/' || !d[21])) return 1;
    }
    return 0;
}

static void sanitize_name(char *s) {
    for (char *p = s; *p; p++)
        if ((unsigned char)*p < 32 || strchr("/\\:*?\"<>|", *p)) *p = '_';
    char *t = s; while (*t == '.' || *t == ' ') t++;
    if (t != s) memmove(s, t, strlen(t) + 1);
    size_t n = strlen(s);
    while (n && (s[n - 1] == ' ' || s[n - 1] == '.')) s[--n] = 0;
    if (n > 200) s[200] = 0;
}

static void pct_decode(char *s) {
    char *o = s;
    for (char *p = s; *p; p++) {
        if (*p == '%' && isxdigit((unsigned char)p[1]) && isxdigit((unsigned char)p[2])) {
            char h[3] = {p[1], p[2], 0}; *o++ = (char)strtol(h, NULL, 16); p += 2;
        } else *o++ = *p;
    }
    *o = 0;
}

static int64_t now_ms(void) { struct timespec t; clock_gettime(CLOCK_MONOTONIC, &t); return (int64_t)t.tv_sec * 1000 + t.tv_nsec / 1000000; }

static Queue *qfind(int id) { for (int i = 0; i < MAX_Q; i++) if (qs[i].used && qs[i].id == id) return &qs[i]; return NULL; }
static Download *dfind(int id) { for (int i = 0; i < MAX_DL; i++) if (dls[i].used && dls[i].id == id) return &dls[i]; return NULL; }

static int64_t seg_done_sum(const Download *d) {
    int64_t s = 0; for (int i = 0; i < d->nseg; i++) s += d->seg[i].pos - d->seg[i].start; return s;
}

/* ------------------------------------------------------------- curl setup */
static void curl_common(CURL *h, const char *url) {
    curl_easy_setopt(h, CURLOPT_URL, url);
    curl_easy_setopt(h, CURLOPT_NOSIGNAL, 1L);
    curl_easy_setopt(h, CURLOPT_FOLLOWLOCATION, 1L);
    curl_easy_setopt(h, CURLOPT_MAXREDIRS, 10L);
    curl_easy_setopt(h, CURLOPT_CONNECTTIMEOUT, 15L);
    curl_easy_setopt(h, CURLOPT_LOW_SPEED_LIMIT, 1024L);
    curl_easy_setopt(h, CURLOPT_LOW_SPEED_TIME, 30L);
    curl_easy_setopt(h, CURLOPT_USERAGENT, "Mozilla/5.0 (PlayStation; PlayStation 5) DM/" DL_VERSION);
    curl_easy_setopt(h, CURLOPT_ACCEPT_ENCODING, "identity");
    curl_easy_setopt(h, CURLOPT_PROTOCOLS_STR, "http,https");
    struct curl_blob blob = { (void *)assets_cacert_pem, assets_cacert_pem_len, CURL_BLOB_COPY };
    curl_easy_setopt(h, CURLOPT_CAINFO_BLOB, &blob);
    curl_easy_setopt(h, CURLOPT_SSL_VERIFYPEER, 1L);
    curl_easy_setopt(h, CURLOPT_SSL_VERIFYHOST, 2L);
}

/* ------------------------------------------------------------------ probe */
typedef struct {
    int status; int64_t total; int resumable; char fname[256]; int abort_body;
} Probe;

static size_t probe_hdr(char *b, size_t sz, size_t n, void *u) {
    Probe *p = u; size_t len = sz * n; char line[600];
    size_t c = len < sizeof line - 1 ? len : sizeof line - 1; memcpy(line, b, c); line[c] = 0;
    if (!strncmp(line, "HTTP/", 5)) {
        char *sp = strchr(line, ' '); p->status = sp ? atoi(sp + 1) : 0;
        p->total = -1; p->resumable = 0; p->fname[0] = 0;       /* new response block (redirect) */
    } else if (!strncasecmp(line, "Content-Range:", 14)) {
        char *sl = strrchr(line, '/');
        if (sl && isdigit((unsigned char)sl[1])) { p->total = atoll(sl + 1); p->resumable = 1; }
    } else if (!strncasecmp(line, "Content-Length:", 15)) {
        if (p->status == 200 && p->total < 0) p->total = atoll(line + 15);
    } else if (!strncasecmp(line, "Content-Disposition:", 20)) {
        char *f = strcasestr(line, "filename*=");
        if (f) {
            f += 10; char *q = strstr(f, "''"); if (q) f = q + 2;
            ssafe(p->fname, sizeof p->fname, f);
        } else if ((f = strcasestr(line, "filename="))) {
            f += 9; if (*f == '"') f++;
            ssafe(p->fname, sizeof p->fname, f);
        }
        char *e = p->fname + strcspn(p->fname, "\";\r\n"); *e = 0;
        pct_decode(p->fname);
    }
    return len;
}
static size_t probe_wr(char *b, size_t sz, size_t n, void *u) {
    Probe *p = u; (void)b;
    if (p->status == 200) { p->abort_body = 1; return 0; }      /* got headers; no ranges -> stop */
    return sz * n;
}

static int probe(Download *d, char *err, size_t en) {
    CURL *h = curl_easy_init(); if (!h) { snprintf(err, en, "curl init failed"); return -1; }
    Probe p = { 0, -1, 0, "", 0 };
    curl_common(h, d->url);
    curl_easy_setopt(h, CURLOPT_RANGE, "0-0");
    curl_easy_setopt(h, CURLOPT_HEADERFUNCTION, probe_hdr);
    curl_easy_setopt(h, CURLOPT_HEADERDATA, &p);
    curl_easy_setopt(h, CURLOPT_WRITEFUNCTION, probe_wr);
    curl_easy_setopt(h, CURLOPT_WRITEDATA, &p);
    curl_easy_setopt(h, CURLOPT_TIMEOUT, 60L);
    CURLcode rc = curl_easy_perform(h);
    char *eff = NULL; curl_easy_getinfo(h, CURLINFO_EFFECTIVE_URL, &eff);
    char effurl[2048]; ssafe(effurl, sizeof effurl, eff);
    long code = 0; curl_easy_getinfo(h, CURLINFO_RESPONSE_CODE, &code);
    curl_easy_cleanup(h);
    if (rc != CURLE_OK && !(rc == CURLE_WRITE_ERROR && p.abort_body)) {
        snprintf(err, en, "Connect failed: %s", curl_easy_strerror(rc)); return -1;
    }
    if (code == 416) { p.total = 0; p.resumable = 0; }
    else if (code >= 400) { snprintf(err, en, "Server returned HTTP %ld", code); return -1; }
    d->total = p.total; d->resumable = p.resumable && p.total > 0;

    if (!d->name[0]) {
        char nm[256]; nm[0] = 0;
        if (p.fname[0]) ssafe(nm, sizeof nm, p.fname);
        else {
            char *u = effurl[0] ? effurl : d->url;
            char tmp[2048]; ssafe(tmp, sizeof tmp, u);
            tmp[strcspn(tmp, "?#")] = 0;
            char *sl = strrchr(tmp, '/'); ssafe(nm, sizeof nm, sl ? sl + 1 : "");
            pct_decode(nm);
        }
        sanitize_name(nm);
        if (!nm[0]) ssafe(nm, sizeof nm, "download");
        ssafe(d->name, sizeof d->name, nm);
    } else sanitize_name(d->name);
    return 0;
}

static int path_taken(const Download *self, const char *path) {
    char part[1100]; snprintf(part, sizeof part, "%s.dmpart", path);
    struct stat st; if (!stat(path, &st) || !stat(part, &st)) return 1;
    for (int i = 0; i < MAX_DL; i++)
        if (dls[i].used && &dls[i] != self && !strcmp(dls[i].path, path)) return 1;
    return 0;
}

/* -------------------------------------------------------- segment worker */
typedef struct {
    Download *d; Seg *s; CURL *h; int ranged, checked, fd, fatal;
    time_t lim_t; curl_off_t lim_v;             /* speed cap last applied to this connection (bytes/s, 0 = none) */
    char err[160];
} Wk;

static size_t wk_write(char *b, size_t sz, size_t n, void *u) {
    Wk *w = u; size_t len = sz * n;
    if (!w->checked) {
        long code = 0; curl_easy_getinfo(w->h, CURLINFO_RESPONSE_CODE, &code);
        w->checked = 1;
        if (code >= 400) { snprintf(w->err, sizeof w->err, "HTTP %ld", code); w->fatal = (code != 408 && code != 429 && code < 500); return 0; }
        if (w->ranged && code != 206) { snprintf(w->err, sizeof w->err, "Server ignored range request"); w->fatal = 1; return 0; }
    }
    if (w->d->stop) return 0;
    size_t take = len;
    if (w->s->end >= 0) {
        int64_t room = w->s->end - w->s->pos + 1;
        if (room <= 0) return len;
        if ((int64_t)take > room) take = (size_t)room;
    }
    size_t off = 0;
    while (off < take) {
        ssize_t r = pwrite(w->fd, b + off, take - off, (off_t)(w->s->pos + (int64_t)off));
        if (r < 0) { if (errno == EINTR) continue; snprintf(w->err, sizeof w->err, "Write error: %s", strerror(errno)); w->fatal = 1; return 0; }
        off += (size_t)r;
    }
    w->s->pos += (int64_t)take;
    return len;
}
/* Speed cap for ONE connection: the global per-download limit and the queue limit (shared by the queue's active downloads), whichever is tighter. */
static curl_off_t conn_limit(const Download *d) {
    int segs = d->nseg > 0 ? d->nseg : 1; curl_off_t best = 0;
    if (S.limit_kbps > 0) best = (curl_off_t)S.limit_kbps * 1024 / segs;
    pthread_mutex_lock(&L);
    Queue *q = qfind(d->qid);
    if (q && q->limit_kbps > 0) {
        int act = 0; for (int i = 0; i < MAX_DL; i++) if (dls[i].used && dls[i].qid == d->qid && dls[i].state == ST_ACTIVE) act++;
        if (act < 1) act = 1;
        curl_off_t ql = (curl_off_t)q->limit_kbps * 1024 / act / segs; if (!best || ql < best) best = ql;
    }
    pthread_mutex_unlock(&L);
    return best && best < 512 ? 512 : best;
}
static int wk_xfer(void *u, curl_off_t a, curl_off_t b, curl_off_t c, curl_off_t d) {
    (void)a; (void)b; (void)c; (void)d; Wk *w = u;
    if (w->d->stop) return 1;
    time_t now = time(NULL);
    if (now != w->lim_t) {                                          /* re-evaluate once a second: other downloads may have started or stopped */
        w->lim_t = now; curl_off_t v = conn_limit(w->d);
        if (v != w->lim_v) { w->lim_v = v; curl_easy_setopt(w->h, CURLOPT_MAX_RECV_SPEED_LARGE, v); }
    }
    return 0;
}

static void *seg_main(void *arg) {
    Wk *w = arg; Download *d = w->d; Seg *s = w->s; int tries = 0, retries = S.retries;
    while (!d->stop) {
        if (s->end >= 0 && s->pos > s->end) return NULL;
        CURL *h = curl_easy_init(); if (!h) { snprintf(w->err, sizeof w->err, "curl init failed"); w->fatal = 1; return NULL; }
        w->h = h; w->checked = 0; w->ranged = d->resumable;
        curl_common(h, d->url);
        char rng[64];
        if (d->resumable) { snprintf(rng, sizeof rng, "%lld-%lld", (long long)s->pos, (long long)s->end); curl_easy_setopt(h, CURLOPT_RANGE, rng); }
        else if (s->pos > 0) { s->pos = s->start; }                  /* cannot resume: restart this stream */
        curl_easy_setopt(h, CURLOPT_WRITEFUNCTION, wk_write);
        curl_easy_setopt(h, CURLOPT_WRITEDATA, w);
        curl_easy_setopt(h, CURLOPT_NOPROGRESS, 0L);
        curl_easy_setopt(h, CURLOPT_XFERINFOFUNCTION, wk_xfer);
        curl_easy_setopt(h, CURLOPT_XFERINFODATA, w);
        w->lim_t = time(NULL); w->lim_v = conn_limit(d);
        if (w->lim_v > 0) curl_easy_setopt(h, CURLOPT_MAX_RECV_SPEED_LARGE, w->lim_v);
        w->err[0] = 0;
        CURLcode rc = curl_easy_perform(h);
        long code = 0; curl_easy_getinfo(h, CURLINFO_RESPONSE_CODE, &code);
        curl_easy_cleanup(h);
        if (d->stop) return NULL;
        if (rc == CURLE_OK && code >= 400 && !w->err[0]) { snprintf(w->err, sizeof w->err, "HTTP %ld", code); w->fatal = (code < 500 && code != 408 && code != 429); }
        if (rc == CURLE_OK && !w->err[0]) {
            if (s->end < 0 || s->pos > s->end) return NULL;           /* finished */
            snprintf(w->err, sizeof w->err, "Connection closed early");
        }
        if (!w->err[0]) snprintf(w->err, sizeof w->err, "%s", curl_easy_strerror(rc));
        if (w->fatal || ++tries > retries) { w->fatal = 1; return NULL; }
        for (int i = 0; i < 10 * tries && !d->stop; i++) usleep(100000);   /* backoff */
    }
    return NULL;
}

/* ------------------------------------------------------- download manager */
static void build_segments(Download *d) {
    int n = S.segments < 1 ? 1 : S.segments > MAX_SEG ? MAX_SEG : S.segments;
    if (!d->resumable || d->total <= 0) {
        d->nseg = 1; d->seg[0].start = 0; d->seg[0].pos = 0; d->seg[0].end = d->total > 0 ? d->total - 1 : -1; return;
    }
    int64_t cap = d->total / SEG_MIN_BYTES; if (cap < 1) cap = 1; if (n > cap) n = (int)cap;
    int64_t chunk = d->total / n;
    for (int i = 0; i < n; i++) {
        d->seg[i].start = d->seg[i].pos = i * chunk;
        d->seg[i].end = (i == n - 1) ? d->total - 1 : (i + 1) * chunk - 1;
    }
    d->nseg = n;
}

static void finish(Download *d, int state, const char *err) {
    char nm[96] = "", why[96] = ""; int nst = -1;
    pthread_mutex_lock(&L);
    if (d->remove_req) {
        char part[1100]; snprintf(part, sizeof part, "%s.dmpart", d->path);
        if (d->path[0]) unlink(part);
        memset(d, 0, sizeof *d);
    } else {
        d->state = state; d->speed = 0; d->running = 0;
        ssafe(d->err, sizeof d->err, err);
        if (state == ST_DONE || state == ST_ERROR) {
            nst = state; snprintf(nm, sizeof nm, "%.90s", d->name[0] ? d->name : d->url); snprintf(why, sizeof why, "%.90s", d->err);
            Queue *fq = qfind(d->qid); if (fq) fq->fin_pending = 1;
        }
    }
    d->running = 0; dirty = 1;
    pthread_mutex_unlock(&L);
    if (nst >= 0 && S.notify >= 2) {
        if (nst == ST_DONE) plat_notify("Download finished: %s", nm); else plat_notify("Download failed: %s\n%s", nm, why);
    }
}

static void *dl_main(void *arg) {
    Download *d = arg; char err[160] = "", part[1100];
    if (d->nseg == 0) {                                           /* first start: probe */
        if (probe(d, err, sizeof err)) { finish(d, ST_ERROR, err); return NULL; }
        if (d->stop) { finish(d, d->stop_to, ""); return NULL; }
        char path[1024]; snprintf(path, sizeof path, "%s/%s", d->dir, d->name);
        pthread_mutex_lock(&L);
        if (path_taken(d, path)) {
            char stem[256], ext[64] = ""; ssafe(stem, sizeof stem, d->name);
            char *dot = strrchr(stem, '.');
            if (dot && dot != stem && strlen(dot) < 12) { ssafe(ext, sizeof ext, dot); *dot = 0; }
            for (int i = 1; i < 1000; i++) {
                snprintf(path, sizeof path, "%s/%s (%d)%s", d->dir, stem, i, ext);
                if (!path_taken(d, path)) break;
            }
            const char *bn = strrchr(path, '/'); ssafe(d->name, sizeof d->name, bn + 1);
        }
        ssafe(d->path, sizeof d->path, path);
        build_segments(d);
        pthread_mutex_unlock(&L);
    }
    if (mkdir_p(d->dir)) { snprintf(err, sizeof err, "Cannot create folder: %s", strerror(errno)); finish(d, ST_ERROR, err); return NULL; }
    snprintf(part, sizeof part, "%s.dmpart", d->path);

    struct stat st; int fresh = stat(part, &st) != 0;
    if (fresh) for (int i = 0; i < d->nseg; i++) d->seg[i].pos = d->seg[i].start;   /* part file vanished */
    if (d->total > 0) {
        struct statvfs vf;
        if (!statvfs(d->dir, &vf)) {
            uint64_t avail = (uint64_t)vf.f_bavail * vf.f_frsize; int64_t need = d->total - seg_done_sum(d);
            if (need > 0 && (uint64_t)need > avail) { finish(d, ST_ERROR, "Not enough free space"); return NULL; }
        }
    }
    int fd = open(part, O_RDWR | O_CREAT, 0644);
    if (fd < 0) { snprintf(err, sizeof err, "Cannot open file: %s", strerror(errno)); finish(d, ST_ERROR, err); return NULL; }
    if (fresh && d->total > 0 && ftruncate(fd, (off_t)d->total)) { snprintf(err, sizeof err, "Cannot allocate file: %s", strerror(errno)); close(fd); finish(d, ST_ERROR, err); return NULL; }

    Wk wk[MAX_SEG]; pthread_t th[MAX_SEG]; int started[MAX_SEG] = {0};
    pthread_attr_t at; pthread_attr_init(&at); pthread_attr_setstacksize(&at, 512 * 1024);
    for (int i = 0; i < d->nseg; i++) {
        memset(&wk[i], 0, sizeof wk[i]); wk[i].d = d; wk[i].s = &d->seg[i]; wk[i].fd = fd;
        if (d->seg[i].end >= 0 && d->seg[i].pos > d->seg[i].end) continue;
        started[i] = !pthread_create(&th[i], &at, seg_main, &wk[i]);
        if (!started[i]) { d->stop = 1; snprintf(err, sizeof err, "Cannot start worker thread"); }
    }
    pthread_attr_destroy(&at);
    for (int i = 0; i < d->nseg; i++) if (started[i]) pthread_join(th[i], NULL);

    int failed = 0;
    for (int i = 0; i < d->nseg; i++) if (wk[i].fatal) { failed = 1; if (!err[0]) ssafe(err, sizeof err, wk[i].err); }
    if (d->stop) {
        if (err[0] && !d->remove_req) { close(fd); finish(d, ST_ERROR, err); return NULL; }
        fsync(fd); close(fd); finish(d, d->stop_to, ""); return NULL;
    }
    if (failed) { close(fd); finish(d, ST_ERROR, err[0] ? err : "Download failed"); return NULL; }
    int ok = 1;
    for (int i = 0; i < d->nseg; i++) if (d->seg[i].end >= 0 && d->seg[i].pos <= d->seg[i].end) ok = 0;
    if (!ok) { close(fd); finish(d, ST_ERROR, "Incomplete download"); return NULL; }
    if (d->total < 0) d->total = seg_done_sum(d);
    else if (d->total > 0 && ftruncate(fd, (off_t)d->total)) {}
    fsync(fd); close(fd);
    if (rename(part, d->path)) { snprintf(err, sizeof err, "Rename failed: %s", strerror(errno)); finish(d, ST_ERROR, err); return NULL; }
    log_msg("[dl] done: %s\n", d->path);
    finish(d, ST_DONE, "");
    return NULL;
}

static void start_dl(Download *d) {                               /* L held */
    d->state = ST_ACTIVE; d->stop = 0; d->running = 1; d->err[0] = 0; d->speed = 0;
    d->last_done = seg_done_sum(d); d->stop_to = ST_PAUSED;
    pthread_t t; pthread_attr_t at; pthread_attr_init(&at);
    pthread_attr_setdetachstate(&at, PTHREAD_CREATE_DETACHED); pthread_attr_setstacksize(&at, 512 * 1024);
    if (pthread_create(&t, &at, dl_main, d)) { d->state = ST_ERROR; d->running = 0; ssafe(d->err, sizeof d->err, "Cannot start thread"); }
    pthread_attr_destroy(&at);
}

/* ------------------------------------------------------------ persistence */
static void path_of(char *out, size_t n, const char *leaf) { snprintf(out, n, "%s/%s", dl_base(), leaf); }

static void save_state(void) {                                    /* L held */
    char p[512], tmp[540]; path_of(p, sizeof p, "state.db"); snprintf(tmp, sizeof tmp, "%s.tmp", p);
    FILE *f = fopen(tmp, "w"); if (!f) return;
    for (int i = 0; i < MAX_Q; i++) if (qs[i].used)
        fprintf(f, "Q\t%d\t%d\t%d\t%s\t%d\t%d\t%d\t%d\t%d\t%d\n", qs[i].id, qs[i].running, qs[i].max, qs[i].name,
                qs[i].sched_on, qs[i].start_min, qs[i].stop_min, qs[i].tz, qs[i].limit_kbps, qs[i].chain);
    for (int i = 0; i < MAX_DL; i++) if (dls[i].used) {
        Download *d = &dls[i];
        fprintf(f, "D\t%d\t%d\t%d\t%ld\t%lld\t%d\t%d\t%lld\t%s\t%s\t%s\t%s\t%s\n", d->id, d->qid, d->state, d->ord,
                (long long)d->total, d->resumable, d->nseg, (long long)d->added, d->url, d->name, d->dir, d->path, d->err);
        for (int k = 0; k < d->nseg; k++)
            fprintf(f, "s\t%lld\t%lld\t%lld\n", (long long)d->seg[k].start, (long long)d->seg[k].end, (long long)d->seg[k].pos);
    }
    fclose(f); rename(tmp, p);
    path_of(p, sizeof p, "settings.cfg"); snprintf(tmp, sizeof tmp, "%s.tmp", p);
    if ((f = fopen(tmp, "w"))) {
        fprintf(f, "segments=%d\nlimit_kbps=%d\nretries=%d\nauto_install_app=%d\nnotify=%d\ndir=%s\n", S.segments, S.limit_kbps, S.retries, S.auto_install_app, S.notify, S.dir);
        fclose(f); rename(tmp, p);
    }
}


static char *field(char **p) { char *s = *p; if (!s) return ""; char *t = strchr(s, '\t'); if (t) { *t = 0; *p = t + 1; } else *p = NULL; return s; }

static void load_state(void) {
    char p[512], line[5000]; FILE *f;
    path_of(p, sizeof p, "settings.cfg");
    if ((f = fopen(p, "r"))) {
        while (fgets(line, sizeof line, f)) {
            line[strcspn(line, "\r\n")] = 0; char *e = strchr(line, '='); if (!e) continue; *e++ = 0;
            if (!strcmp(line, "segments")) S.segments = atoi(e);
            else if (!strcmp(line, "limit_kbps")) S.limit_kbps = atoi(e);
            else if (!strcmp(line, "retries")) S.retries = atoi(e);
            else if (!strcmp(line, "auto_install_app")) S.auto_install_app = atoi(e);
            else if (!strcmp(line, "notify")) S.notify = atoi(e);
            else if (!strcmp(line, "dir")) { char t[512]; snprintf(t, sizeof t, "%s", e); if (eng_dir_allowed(t)) ssafe(S.dir, sizeof S.dir, t); }
        }
        fclose(f);
    }
    path_of(p, sizeof p, "state.db");
    if (!(f = fopen(p, "r"))) return;
    Download *cur = NULL; int want_seg = 0;
    while (fgets(line, sizeof line, f)) {
        line[strcspn(line, "\r\n")] = 0; char *c = line; char *tag = field(&c);
        if (!strcmp(tag, "Q")) {
            for (int i = 0; i < MAX_Q; i++) if (!qs[i].used) {
                qs[i].used = 1; qs[i].id = atoi(field(&c)); qs[i].running = atoi(field(&c)); qs[i].max = atoi(field(&c));
                ssafe(qs[i].name, sizeof qs[i].name, field(&c));
                qs[i].sched_on = atoi(field(&c)); qs[i].start_min = atoi(field(&c)); qs[i].stop_min = atoi(field(&c)); qs[i].tz = atoi(field(&c)); qs[i].limit_kbps = atoi(field(&c)); qs[i].chain = atoi(field(&c)); qs[i].last_in = -1;
                if (qs[i].id >= next_qid) next_qid = qs[i].id + 1;
                break;
            }
        } else if (!strcmp(tag, "D")) {
            cur = NULL;
            for (int i = 0; i < MAX_DL; i++) if (!dls[i].used) { cur = &dls[i]; break; }
            if (!cur) continue;
            memset(cur, 0, sizeof *cur); cur->used = 1;
            cur->id = atoi(field(&c)); cur->qid = atoi(field(&c)); cur->state = atoi(field(&c)); cur->ord = atol(field(&c));
            cur->total = atoll(field(&c)); cur->resumable = atoi(field(&c)); cur->nseg = atoi(field(&c)); cur->added = (time_t)atoll(field(&c));
            ssafe(cur->url, sizeof cur->url, field(&c)); ssafe(cur->name, sizeof cur->name, field(&c));
            ssafe(cur->dir, sizeof cur->dir, field(&c)); ssafe(cur->path, sizeof cur->path, field(&c)); ssafe(cur->err, sizeof cur->err, field(&c));
            if (cur->state == ST_ACTIVE) cur->state = ST_QUEUED;       /* interrupted by shutdown: resume later */
            if (cur->nseg < 0 || cur->nseg > MAX_SEG) cur->nseg = 0;
            if (cur->id >= next_id) next_id = cur->id + 1;
            if (cur->ord >= next_ord) next_ord = cur->ord + 1;
            if (!eng_dir_allowed(cur->dir)) { cur->state = ST_ERROR; ssafe(cur->err, sizeof cur->err, "Folder not allowed"); }
            want_seg = cur->nseg; cur->nseg = 0;                        /* 's' lines that follow refill it */
        } else if (!strcmp(tag, "s") && cur && cur->nseg < want_seg) {
            Seg *s = &cur->seg[cur->nseg++];
            s->start = atoll(field(&c)); s->end = atoll(field(&c)); s->pos = atoll(field(&c));
        }
    }
    fclose(f);
    for (int i = 0; i < MAX_DL; i++) if (dls[i].used) { dls[i].stop_to = ST_PAUSED; dls[i].last_done = seg_done_sum(&dls[i]); }
}

/* ------------------------------------------------------------- engine API */
/* dup (optional): 1 = already listed (not added, existing id returned), 2 = downloaded before (added anyway) */
int eng_add(const char *url, const char *name, const char *dir, int qid, char *err, size_t en, int *dup) {
    if (dup) *dup = 0;
    if (!url) { snprintf(err, en, "Missing URL"); return -1; }
    char u[2048]; size_t n = 0;
    while (*url && isspace((unsigned char)*url)) url++;
    for (; *url && n < sizeof u - 4; url++) {
        if ((unsigned char)*url < 32 && !isspace((unsigned char)*url)) { snprintf(err, en, "Invalid characters in URL"); return -1; }
        if (*url == ' ') { u[n++] = '%'; u[n++] = '2'; u[n++] = '0'; } else if (!isspace((unsigned char)*url)) u[n++] = *url;
    }
    u[n] = 0;
    if (strncasecmp(u, "http://", 7) && strncasecmp(u, "https://", 8)) { snprintf(err, en, "Only http:// and https:// links are supported"); return -1; }
    if (n < 10) { snprintf(err, en, "URL too short"); return -1; }
    char d2[512]; ssafe(d2, sizeof d2, (dir && dir[0]) ? dir : "");
    pthread_mutex_lock(&L);
    if (!d2[0]) ssafe(d2, sizeof d2, S.dir);
    if (!eng_dir_allowed(d2)) { pthread_mutex_unlock(&L); snprintf(err, en, "Folder not allowed (use /data/DownloadMGR or <drive>/DownloadMGR)"); return -1; }
    if (!qfind(qid)) qid = 1;
    char want_name[256]; ssafe(want_name, sizeof want_name, name); sanitize_name(want_name);
    for (int i = 0; i < MAX_DL; i++)                                  /* same unfinished link already listed (in any queue; same name if one was given): do not add it again */
        if (dls[i].used && dls[i].state != ST_DONE && !strcmp(dls[i].url, u) &&
            (!want_name[0] || !strcmp(dls[i].name, want_name))) { int id = dls[i].id; pthread_mutex_unlock(&L); if (dup) *dup = 1; return id; }
    for (int i = 0; i < MAX_DL; i++)                                  /* downloaded before: still added, but the caller can warn */
        if (dls[i].used && dls[i].state == ST_DONE && !strcmp(dls[i].url, u)) { if (dup) *dup = 2; break; }
    Download *d = NULL;
    for (int i = 0; i < MAX_DL; i++) if (!dls[i].used) { d = &dls[i]; break; }
    if (!d) { pthread_mutex_unlock(&L); snprintf(err, en, "Download list is full (%d)", MAX_DL); return -1; }
    memset(d, 0, sizeof *d);
    d->used = 1; d->id = next_id++; d->qid = qid; d->state = ST_QUEUED; d->ord = next_ord++; d->total = 0; d->added = time(NULL);
    ssafe(d->url, sizeof d->url, u); ssafe(d->dir, sizeof d->dir, d2); ssafe(d->name, sizeof d->name, name);
    sanitize_name(d->name); d->stop_to = ST_PAUSED;
    int id = d->id; dirty = 1;
    pthread_mutex_unlock(&L);
    return id;
}

static void reset_progress(Download *d, int del_part) {
    if (del_part && d->path[0]) { char part[1100]; snprintf(part, sizeof part, "%s.dmpart", d->path); unlink(part); }
    d->nseg = 0; d->total = 0; d->resumable = 0; d->path[0] = 0; d->last_done = 0; d->speed = 0;
}

int eng_item_action(int id, const char *act, const char *arg) {
    int rc = 0;
    pthread_mutex_lock(&L);
    Download *d = dfind(id);
    if (!d || !act) { pthread_mutex_unlock(&L); return -1; }
    if (!strcmp(act, "pause")) {
        if (d->state == ST_ACTIVE) { d->stop_to = ST_PAUSED; d->stop = 1; } else if (d->state == ST_QUEUED) d->state = ST_PAUSED;
    } else if (!strcmp(act, "resume")) {
        if (d->state == ST_PAUSED || d->state == ST_ERROR) { d->state = ST_QUEUED; d->err[0] = 0; }
    } else if (!strcmp(act, "restart")) {
        if (d->state == ST_ACTIVE) rc = -1;                           /* pause it first */
        else {
            if (d->state == ST_DONE) { d->path[0] = 0; d->name[0] = 0; } /* keep the finished file; fetch again under a new name */
            reset_progress(d, 1); d->state = ST_QUEUED; d->err[0] = 0;
        }
    } else if (!strcmp(act, "remove") || !strcmp(act, "delete")) {
        int del = !strcmp(act, "delete");
        if (del && d->state == ST_DONE && d->path[0]) unlink(d->path);
        if (d->running) { d->remove_req = 1; d->stop = 1; }
        else { char part[1100]; snprintf(part, sizeof part, "%s.dmpart", d->path); if (d->path[0] && d->state != ST_DONE) unlink(part); memset(d, 0, sizeof *d); }
    } else if (!strcmp(act, "up") || !strcmp(act, "down")) {
        int up = !strcmp(act, "up"); Download *best = NULL;
        for (int i = 0; i < MAX_DL; i++) {
            Download *o = &dls[i]; if (!o->used || o == d || o->qid != d->qid) continue;
            if (up ? (o->ord < d->ord && (!best || o->ord > best->ord)) : (o->ord > d->ord && (!best || o->ord < best->ord))) best = o;
        }
        if (best) { long t = best->ord; best->ord = d->ord; d->ord = t; }
    } else if (!strcmp(act, "move")) {
        int q = arg ? atoi(arg) : 0; if (qfind(q)) { d->qid = q; d->ord = next_ord++; } else rc = -1;
    } else rc = -1;
    dirty = 1;
    pthread_mutex_unlock(&L);
    return rc;
}

int eng_queue_create(const char *name) {
    int id = -1; pthread_mutex_lock(&L);
    for (int i = 0; i < MAX_Q; i++) if (!qs[i].used) {
        qs[i].used = 1; qs[i].id = id = next_qid++; qs[i].running = 1; qs[i].max = 2; qs[i].limit_kbps = 0; qs[i].chain = 0; qs[i].last_in = -1;
        ssafe(qs[i].name, sizeof qs[i].name, (name && name[0]) ? name : "New Queue"); break;
    }
    dirty = 1; pthread_mutex_unlock(&L); return id;
}

int eng_queue_update(int qid, const char *name, int max, int running, int limit_kbps, int chain) {
    pthread_mutex_lock(&L); Queue *q = qfind(qid);
    if (!q) { pthread_mutex_unlock(&L); return -1; }
    if (name && name[0]) ssafe(q->name, sizeof q->name, name);
    if (max >= 1 && max <= 8) q->max = max;
    if (limit_kbps >= 0 && limit_kbps <= 1000000) q->limit_kbps = limit_kbps;
    if (chain == 0 || (chain > 0 && chain != qid && qfind(chain))) q->chain = chain;
    if (running >= 0) {
        q->running = running ? 1 : 0;
        if (!q->running)                                              /* stopping a queue halts its active items; they resume when started */
            for (int i = 0; i < MAX_DL; i++) if (dls[i].used && dls[i].qid == qid && dls[i].state == ST_ACTIVE) { dls[i].stop_to = ST_QUEUED; dls[i].stop = 1; }
    }
    dirty = 1; pthread_mutex_unlock(&L); return 0;
}

int eng_queue_schedule(int qid, int on, int start_min, int stop_min, int tz) {
    if (start_min < 0 || start_min > 1439 || stop_min < 0 || stop_min > 1439 || tz < -840 || tz > 840) return -1;
    pthread_mutex_lock(&L); Queue *q = qfind(qid);
    if (!q) { pthread_mutex_unlock(&L); return -1; }
    q->sched_on = on ? 1 : 0; q->start_min = start_min; q->stop_min = stop_min; q->tz = tz; q->last_in = -1;
    dirty = 1; pthread_mutex_unlock(&L); return 0;
}

int eng_queue_delete(int qid) {
    pthread_mutex_lock(&L); Queue *q = qfind(qid);
    if (!q || qid == 1) { pthread_mutex_unlock(&L); return -1; }
    for (int i = 0; i < MAX_DL; i++) if (dls[i].used && dls[i].qid == qid) dls[i].qid = 1;
    memset(q, 0, sizeof *q); dirty = 1; pthread_mutex_unlock(&L); return 0;
}

void eng_settings_get(Settings *o) { pthread_mutex_lock(&L); *o = S; pthread_mutex_unlock(&L); }

int eng_settings_set(const Settings *in, char *err, size_t en) {
    if (!eng_dir_allowed(in->dir)) { snprintf(err, en, "Folder not allowed"); return -1; }
    pthread_mutex_lock(&L);
    S.segments = in->segments < 1 ? 1 : in->segments > MAX_SEG ? MAX_SEG : in->segments;
    S.limit_kbps = in->limit_kbps < 0 ? 0 : in->limit_kbps;
    S.retries = in->retries < 0 ? 0 : in->retries > 20 ? 20 : in->retries;
    S.auto_install_app = in->auto_install_app ? 1 : 0;
    S.notify = in->notify < 0 ? 0 : in->notify > 2 ? 2 : in->notify;
    ssafe(S.dir, sizeof S.dir, in->dir); dirty = 1;
    pthread_mutex_unlock(&L); return 0;
}

/* ------------------------------------------------------------------- JSON */
typedef struct { char *b; size_t n, cap; } SB;
#define SBL(s, lit) sb_put((s), lit, sizeof(lit) - 1)
static void sb_put(SB *s, const char *t, size_t l) {
    if (s->n + l + 1 > s->cap) { s->cap = (s->n + l + 1) * 2 + 256; s->b = realloc(s->b, s->cap); }
    memcpy(s->b + s->n, t, l); s->n += l; s->b[s->n] = 0;
}
static void sb_f(SB *s, const char *fmt, ...) {
    char t[512]; va_list a; va_start(a, fmt); int l = vsnprintf(t, sizeof t, fmt, a); va_end(a);
    if (l >= (int)sizeof t) l = (int)sizeof t - 1;
    sb_put(s, t, (size_t)l);
}
static void sb_str(SB *s, const char *v) {
    sb_put(s, "\"", 1);
    for (; *v; v++) {
        unsigned char c = (unsigned char)*v;
        if (c == '"' || c == '\\') { char e[2] = {'\\', (char)c}; sb_put(s, e, 2); }
        else if (c < 32) { char e[8]; snprintf(e, sizeof e, "\\u%04x", c); sb_put(s, e, 6); }
        else sb_put(s, (char *)&c, 1);
    }
    sb_put(s, "\"", 1);
}
static const char *state_name(int s) { static const char *n[] = {"queued", "active", "paused", "done", "error"}; return n[s]; }

static int cmp_ord(const void *a, const void *b) { long x = (*(Download *const *)a)->ord, y = (*(Download *const *)b)->ord; return x < y ? -1 : x > y; }

char *eng_state_json(void) {
    SB s = {0}; char ip[64] = "";
    plat_local_ip(ip, sizeof ip);
    pthread_mutex_lock(&L);
    sb_f(&s, "{\"version\":\"%s\",\"port\":%d,\"ip\":", DL_VERSION, DL_PORT); sb_str(&s, ip);
    sb_f(&s, ",\"token\":\"%s\",\"settings\":{\"segments\":%d,\"limit_kbps\":%d,\"retries\":%d,\"auto_install_app\":%d,\"notify\":%d,\"dir\":", token, S.segments, S.limit_kbps, S.retries, S.auto_install_app, S.notify);
    sb_str(&s, S.dir);
    struct statvfs vf; long long freeb = -1; if (!statvfs(S.dir, &vf) || !statvfs(dl_base(), &vf)) freeb = (long long)vf.f_bavail * vf.f_frsize;
    sb_f(&s, "},\"free\":%lld,\"queues\":[", freeb);
    int first = 1;
    for (int i = 0; i < MAX_Q; i++) if (qs[i].used) {
        int act = 0, que = 0, done = 0; double sp = 0;
        for (int k = 0; k < MAX_DL; k++) if (dls[k].used && dls[k].qid == qs[i].id) {
            if (dls[k].state == ST_ACTIVE) { act++; sp += dls[k].speed; } else if (dls[k].state == ST_QUEUED) que++; else if (dls[k].state == ST_DONE) done++;
        }
        sb_f(&s, "%s{\"id\":%d,\"running\":%s,\"max\":%d,\"active\":%d,\"queued\":%d,\"done\":%d,\"speed\":%.0f,\"sched_on\":%d,\"start\":%d,\"stop\":%d,\"limit\":%d,\"chain\":%d,\"name\":", first ? "" : ",", qs[i].id, qs[i].running ? "true" : "false", qs[i].max, act, que, done, sp,
             qs[i].sched_on, qs[i].start_min, qs[i].stop_min, qs[i].limit_kbps, qs[i].chain);
        sb_str(&s, qs[i].name); sb_put(&s, "}", 1); first = 0;
    }
    sb_put(&s, "],\"items\":[", 11);
    Download *ord[MAX_DL]; int n = 0;
    for (int i = 0; i < MAX_DL; i++) if (dls[i].used) ord[n++] = &dls[i];
    qsort(ord, (size_t)n, sizeof ord[0], cmp_ord);
    for (int i = 0; i < n; i++) {
        Download *d = ord[i]; int64_t done = d->state == ST_DONE ? d->total : seg_done_sum(d);
        long eta = (d->state == ST_ACTIVE && d->speed > 1 && d->total > 0) ? (long)((d->total - done) / d->speed) : -1;
        sb_f(&s, "%s{\"id\":%d,\"q\":%d,\"state\":\"%s\",\"total\":%lld,\"done\":%lld,\"speed\":%.0f,\"eta\":%ld,\"segs\":%d,\"resumable\":%s,\"name\":",
             i ? "," : "", d->id, d->qid, state_name(d->state), (long long)d->total, (long long)done, d->speed, eta, d->nseg, d->resumable ? "true" : "false");
        sb_str(&s, d->name[0] ? d->name : "(resolving...)"); sb_put(&s, ",\"url\":", 7); sb_str(&s, d->url);
        sb_put(&s, ",\"dir\":", 7); sb_str(&s, d->dir); sb_put(&s, ",\"err\":", 7); sb_str(&s, d->err); sb_put(&s, "}", 1);
    }
    sb_put(&s, "]}", 2);
    pthread_mutex_unlock(&L);
    return s.b;
}

static int is_dir(const char *p) { struct stat st; return !stat(p, &st) && S_ISDIR(st.st_mode); }

/* Drive i (0-7 = /mnt/usbN, 8-11 = /mnt/extN): returns <drive>/DownloadMGR in out, creating it when the drive is mounted. */
static int drive_root(int i, char *out, size_t n) {
    char b[32];
    if (i < 8) snprintf(b, sizeof b, "/mnt/usb%d", i); else snprintf(b, sizeof b, "/mnt/ext%d", i - 8);
    if (!is_dir(b)) return 0;
    snprintf(out, n, "%s/DownloadMGR", b);
    if (is_dir(out)) return 1;
    return !mkdir(out, 0755) || is_dir(out);
}
/* Folders the file manager may start from: the data root plus mounted USB / extended drives. */
static void roots_json(SB *s) {
    char p[600]; int first = 1;
    SBL(s, "{\"path\":\"\",\"parent\":\"\",\"roots\":true,\"dirs\":[");
    for (int i = -1; i < 12; i++) {
        if (i < 0) { snprintf(p, sizeof p, "%s", dl_base()); if (!is_dir(p)) continue; }
        else if (!drive_root(i, p, sizeof p)) continue;
        if (first) SBL(s, "{\"n\":"); else SBL(s, ",{\"n\":"); sb_str(s, p); SBL(s, ",\"p\":"); sb_str(s, p); SBL(s, "}"); first = 0;
    }
    SBL(s, "]}");
}
static int cmp_name(const void *a, const void *b) { return strcasecmp(*(char *const *)a, *(char *const *)b); }
char *eng_ls_json(const char *path, char *err, size_t en) {
    SB s = {0};
    if (!path || !*path) { roots_json(&s); return s.b; }
    char d[520]; snprintf(d, sizeof d, "%s", path);
    size_t dl = strlen(d); if (dl > 1 && d[dl - 1] == '/') d[dl - 1] = 0;
    if (!eng_dir_allowed(d)) { snprintf(err, en, "Folder not allowed"); return NULL; }
    DIR *dir = opendir(d); if (!dir) { snprintf(err, en, "Cannot open folder: %s", strerror(errno)); return NULL; }
    char *names[500]; int n = 0; struct dirent *e;
    while ((e = readdir(dir)) && n < 500) {
        if (e->d_name[0] == '.' || strlen(e->d_name) > 200) continue;
        char full[760]; snprintf(full, sizeof full, "%s/%s", d, e->d_name);
        if (is_dir(full)) names[n++] = strdup(e->d_name);
    }
    closedir(dir); qsort(names, (size_t)n, sizeof names[0], cmp_name);
    char par[520]; snprintf(par, sizeof par, "%s", d); char *sl = strrchr(par, '/'); if (sl && sl != par) *sl = 0; else par[0] = 0;
    if (!par[0] || !eng_dir_allowed(par)) par[0] = 0;               /* above a root: back to the root list */
    SBL(&s, "{\"path\":"); sb_str(&s, d); SBL(&s, ",\"parent\":"); sb_str(&s, par); SBL(&s, ",\"roots\":false,\"dirs\":[");
    for (int i = 0; i < n; i++) {
        char full[760]; snprintf(full, sizeof full, "%s/%s", d, names[i]);
        if (i) SBL(&s, ",{\"n\":"); else SBL(&s, "{\"n\":"); sb_str(&s, names[i]); SBL(&s, ",\"p\":"); sb_str(&s, full); SBL(&s, "}"); free(names[i]);
    }
    SBL(&s, "]}"); return s.b;
}
int eng_mkdir(const char *parent, const char *name, char *out, size_t on, char *err, size_t en) {
    char nm[256], full[800];
    if (!parent || !name || !eng_dir_allowed(parent)) { snprintf(err, en, "Folder not allowed"); return -1; }
    snprintf(nm, sizeof nm, "%s", name); sanitize_name(nm);
    if (!nm[0]) { snprintf(err, en, "Enter a folder name"); return -1; }
    snprintf(full, sizeof full, "%s/%s", parent, nm);
    if (!eng_dir_allowed(full)) { snprintf(err, en, "Folder not allowed"); return -1; }
    if (mkdir(full, 0755) && errno != EEXIST) { snprintf(err, en, "Cannot create folder: %s", strerror(errno)); return -1; }
    snprintf(out, on, "%s", full); return 0;
}

/* ------------------------------------------------------- link grabber */
#define SCAN_MAX_BYTES (3 * 1024 * 1024)
#define SCAN_MAX_LINKS 4000      /* kept while parsing: big site menus can hold 600+ links before the real downloads */
#define SCAN_OUT_FILES 1500     /* sent to the UI: every file link (up to this many)... */
#define SCAN_OUT_OTHER 300      /* ...plus this many other links for 'show all links' */
typedef struct { char *b; size_t n; int over, bin; CURL *h; } Page;
static size_t page_cb(char *p, size_t sz, size_t nm, void *u) {
    Page *pg = u; size_t l = sz * nm;
    if (!pg->n && !pg->bin) {                                      /* first data: refuse real files (zip, iso, video...) instead of reading 3 MB of them */
        char *ct = NULL; curl_easy_getinfo(pg->h, CURLINFO_CONTENT_TYPE, &ct);
        if (ct && strncasecmp(ct, "text/", 5) && !strcasestr(ct, "html") && !strcasestr(ct, "json") && !strcasestr(ct, "xml") && !strcasestr(ct, "javascript")) { pg->bin = 1; return 0; }
        if (memchr(p, 0, l < 512 ? l : 512)) { pg->bin = 1; return 0; }        /* NUL bytes: binary data whatever the server claims */
    }
    if (pg->n + l > SCAN_MAX_BYTES) { l = SCAN_MAX_BYTES - pg->n; pg->over = 1; }
    char *nb = realloc(pg->b, pg->n + l + 1); if (!nb) return 0;
    pg->b = nb; memcpy(pg->b + pg->n, p, l); pg->n += l; pg->b[pg->n] = 0;
    return pg->over ? 0 : sz * nm;                      /* returning less aborts the transfer: we have enough */
}

static const char *FILE_EXT[] = { "zip","rar","7z","tar","gz","tgz","bz2","xz","iso","bin","pkg","elf","exe","msi","dmg","apk","ipa",
    "mp4","mkv","avi","mov","wmv","webm","mp3","flac","wav","ogg","m4a","pdf","epub","cbz","cbr","torrent","rpm","deb","img","vpk","rap","pup", NULL };

static int is_file_ext(const char *url);
static int ext_in_query(const char *url) {                         /* download.php?file=game.part1.rar */
    const char *q = strchr(url, '?'); if (!q) return 0;
    char t[1100]; snprintf(t, sizeof t, "%s", q + 1);
    for (char *tok = strtok(t, "&"); tok; tok = strtok(NULL, "&")) {
        char *v = strchr(tok, '='); if (!v) continue; v++; pct_decode(v);
        if (is_file_ext(v)) return 1;
    }
    return 0;
}
/* Fallback when a page has no direct file links: links that look like a download button (dl.host, /download/, /get/, ...) */
static int likely_dl(const char *u, const char *page_origin) {
    char t[2100]; size_t i = 0; for (; u[i] && i < sizeof t - 1; i++) t[i] = (char)tolower((unsigned char)u[i]); t[i] = 0;
    char *h = strstr(t, "://"); if (!h) return 0; h += 3;
    size_t hl = strcspn(h, "/?#"); char *path = h + hl;
    static const char *SKIP[] = { "css","js","png","jpg","jpeg","gif","svg","ico","webp","woff","woff2","ttf","xml","json","txt","avif", NULL };
    size_t pe = strcspn(path, "?#"); const char *x = path + pe; while (x > path && x[-1] != '.' && x[-1] != '/') x--;
    if (x > path && x[-1] == '.') { char ext[12]; size_t l = (size_t)(path + pe - x); if (l && l < sizeof ext) { memcpy(ext, x, l); ext[l] = 0; for (int k = 0; SKIP[k]; k++) if (!strcmp(ext, SKIP[k])) return 0; } }
    int same = 0;                                                      /* link to the page's own site: only path hints count, not the host name */
    if (page_origin) { const char *po = strstr(page_origin, "://"); po = po ? po + 3 : page_origin; size_t pl = strcspn(po, "/?#"); char ph[300]; snprintf(ph, sizeof ph, "%.*s", (int)pl, po); for (char *c = ph; *c; c++) *c = (char)tolower((unsigned char)*c); same = (strlen(ph) == hl && !strncmp(ph, h, hl)); }
    if (!same && !strncmp(h, "dl", 2) && (isdigit((unsigned char)h[2]) || h[2] == '.' || h[2] == '-') && hl > 4) return 1;
    if (!same) { char sv = h[hl]; h[hl] = 0; int dl = strstr(h, "download") != NULL; h[hl] = sv; if (dl && hl > 8) return 1; }
    static const char *HINT[] = { "/dl/", "/download", "/get/", "/getfile", "/file/", "/files/", "/go/", "/out/", "/link/", "/attachment", "download.php", "dl.php", "go.php", NULL };
    for (int k = 0; HINT[k]; k++) if (strstr(path, HINT[k])) return 1;
    return 0;
}
static int is_file_ext(const char *url) {
    size_t n = strcspn(url, "?#"); const char *e = url + n;
    while (e > url && e[-1] != '.' && e[-1] != '/') e--;
    if (e == url || e[-1] != '.') return 0;
    char x[12]; size_t l = (size_t)(url + n - e); if (!l || l >= sizeof x) return 0;
    for (size_t i = 0; i < l; i++) { x[i] = (char)tolower((unsigned char)e[i]); }
    x[l] = 0;
    for (int i = 0; FILE_EXT[i]; i++) if (!strcmp(x, FILE_EXT[i])) return 1;
    return 0;
}
static int is_file_link(const char *url) { return is_file_ext(url) || ext_in_query(url); }

static void html_unescape(char *s) {                    /* &amp; &quot; &#39; ... only what shows up in URLs */
    char *o = s;
    for (char *p = s; *p; ) {
        if (!strncmp(p, "&amp;", 5)) { *o++ = '&'; p += 5; }
        else if (!strncmp(p, "&quot;", 6)) { *o++ = '"'; p += 6; }
        else if (!strncmp(p, "&#39;", 5) || !strncmp(p, "&#x27;", 6)) { *o++ = '\''; p += p[2] == '3' ? 5 : 6; }
        else *o++ = *p++;
    }
    *o = 0;
}

typedef struct { char *u[SCAN_MAX_LINKS]; int n, truncated; char origin[300], dir[1100], scheme[12]; } Links;


static void encode_url(char *u, size_t n) {                        /* spaces, quotes and non-ASCII bytes -> %XX (existing %XX is kept) */
    char t[3400]; size_t o = 0;
    for (const unsigned char *p = (const unsigned char *)u; *p && o + 4 < sizeof t; p++) {
        if (*p <= 32 || *p >= 127 || strchr("\"<>\\^`{|}", *p)) o += (size_t)snprintf(t + o, 4, "%%%02X", *p); else t[o++] = (char)*p;
    }
    t[o] = 0; snprintf(u, n, "%s", t);
}
/* Resolve a link found in the page against the page address. Returns 0 when it is not an http(s) link. */
static int resolve_url(const Links *L, const char *ref0, char *url, size_t un) {
    char ref[2100]; size_t rl = strlen(ref0);
    while (rl && isspace((unsigned char)*ref0)) { ref0++; rl--; }
    if (!rl || rl >= sizeof ref) return 0;
    memcpy(ref, ref0, rl); ref[rl] = 0;
    while (rl && isspace((unsigned char)ref[rl - 1])) ref[--rl] = 0;
    html_unescape(ref);
    char *hash = strchr(ref, '#'); if (hash) *hash = 0;
    if (!ref[0] || !strncasecmp(ref, "javascript:", 11) || !strncasecmp(ref, "mailto:", 7) || !strncasecmp(ref, "data:", 5) || !strncasecmp(ref, "tel:", 4) || !strncasecmp(ref, "blob:", 5)) return 0;
    char t[3400];
    if (!strncasecmp(ref, "http://", 7) || !strncasecmp(ref, "https://", 8)) snprintf(t, sizeof t, "%s", ref);
    else if (ref[0] == '/' && ref[1] == '/') snprintf(t, sizeof t, "%s:%s", L->scheme, ref);
    else if (ref[0] == '/') snprintf(t, sizeof t, "%s%s", L->origin, ref);
    else if (strchr(ref, ':') && strchr(ref, ':') < ref + strcspn(ref, "/?")) return 0;       /* some other scheme */
    else snprintf(t, sizeof t, "%s%s", L->dir, ref[0] == '.' && ref[1] == '/' ? ref + 2 : ref);
    encode_url(t, sizeof t);
    if (strlen(t) < 10 || strlen(t) > 2000 || strlen(t) >= un) return 0;
    snprintf(url, un, "%s", t); return 1;
}
static void links_add(Links *L, const char *ref0) {
    char url[3400];
    if (!resolve_url(L, ref0, url, sizeof url)) return;
    for (int i = 0; i < L->n; i++) if (!strcmp(L->u[i], url)) return;
    if (L->n >= SCAN_MAX_LINKS) { L->truncated = 1; return; }
    L->u[L->n++] = strdup(url);
}
static void set_base(Links *L, const char *base_url) {              /* split an address into scheme / origin / directory */
    const char *sch = strstr(base_url, "://"); size_t sl = sch ? (size_t)(sch - base_url) : 4; if (sl >= sizeof L->scheme) sl = sizeof L->scheme - 1;
    memcpy(L->scheme, base_url, sl); L->scheme[sl] = 0;
    const char *hs = sch ? sch + 3 : base_url; size_t ol = (size_t)(hs - base_url) + strcspn(hs, "/?#");
    snprintf(L->origin, sizeof L->origin, "%.*s", (int)ol, base_url);
    size_t pl = strcspn(base_url + ol, "?#"); char dirbuf[1100]; snprintf(dirbuf, sizeof dirbuf, "%.*s", (int)(ol + pl), base_url);
    char *ls = strrchr(dirbuf + ol, '/'); if (ls) ls[1] = 0; else snprintf(dirbuf, sizeof dirbuf, "%s/", L->origin);
    snprintf(L->dir, sizeof L->dir, "%s", dirbuf);
}

/* returns malloc'd JSON {"ok":true,"page":..,"links":[{"u","n","f"}]} or NULL + err */
char *eng_scan(const char *url_in, char *err, size_t en) {
    char url[2108] = ""; if (url_in) snprintf(url, sizeof url, "%s", url_in);
    for (size_t l = strlen(url); l; l = strlen(url)) {                 /* pasted junk: trailing > " ' , ) or %3E / %22 (from chat apps / markdown) */
        if (strchr(">\"'<,) \t\r\n", url[l - 1])) url[l - 1] = 0;
        else if (l > 3 && !strcasecmp(url + l - 3, "%3E")) url[l - 3] = 0;
        else if (l > 3 && !strcasecmp(url + l - 3, "%22")) url[l - 3] = 0;
        else break;
    }
    if (!url[0] || (strncasecmp(url, "http://", 7) && strncasecmp(url, "https://", 8)) || strlen(url) > 2000) { snprintf(err, en, "Enter a full page address starting with http:// or https://"); return NULL; }
    CURL *h = curl_easy_init(); if (!h) { snprintf(err, en, "Out of memory"); return NULL; }
    Page pg = {0}; pg.h = h;
    curl_common(h, url);
    curl_easy_setopt(h, CURLOPT_ACCEPT_ENCODING, "");
    curl_easy_setopt(h, CURLOPT_TIMEOUT, 25L);
    /* many download sites hide their links (or answer a bot challenge) when the client does not look like a browser */
    curl_easy_setopt(h, CURLOPT_USERAGENT, "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36");
    struct curl_slist *hd = curl_slist_append(NULL, "Accept: text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8");
    hd = curl_slist_append(hd, "Accept-Language: en-US,en;q=0.9,fa;q=0.8");
    curl_easy_setopt(h, CURLOPT_HTTPHEADER, hd);
    curl_easy_setopt(h, CURLOPT_WRITEFUNCTION, page_cb);
    curl_easy_setopt(h, CURLOPT_WRITEDATA, &pg);
    CURLcode rc = curl_easy_perform(h);
    if (rc == CURLE_COULDNT_RESOLVE_HOST) {                            /* console DNS hiccup: retry once, then IPv4 only, then with "www." for a bare domain */
        log_msg("[scan] could not resolve host, retrying (IPv4)\n");
        curl_easy_setopt(h, CURLOPT_IPRESOLVE, (long)CURL_IPRESOLVE_V4); rc = curl_easy_perform(h);
        const char *hs = strstr(url, "://"); hs = hs ? hs + 3 : url; size_t hl = strcspn(hs, "/?#:"); int dots = 0; for (size_t i = 0; i < hl; i++) if (hs[i] == '.') dots++;
        if (rc == CURLE_COULDNT_RESOLVE_HOST && dots == 1 && strncasecmp(hs, "www.", 4)) {
            char wu[2200]; snprintf(wu, sizeof wu, "%.*swww.%s", (int)(hs - url), url, hs);
            log_msg("[scan] retrying as %s\n", wu); curl_easy_setopt(h, CURLOPT_URL, wu); rc = curl_easy_perform(h);
        }
    }
    long code = 0; char *eff = NULL, *ctp = NULL; curl_easy_getinfo(h, CURLINFO_RESPONSE_CODE, &code); curl_easy_getinfo(h, CURLINFO_EFFECTIVE_URL, &eff); curl_easy_getinfo(h, CURLINFO_CONTENT_TYPE, &ctp);
    char ctype[80]; snprintf(ctype, sizeof ctype, "%s", ctp ? ctp : "");
    curl_slist_free_all(hd);
    if (pg.bin) { snprintf(err, en, "That address is a file, not a web page. Use Add links to download it."); free(pg.b); curl_easy_cleanup(h); return NULL; }
    if ((rc != CURLE_OK && !(rc == CURLE_WRITE_ERROR && pg.over)) || code >= 400 || !pg.b) {
        if (rc != CURLE_OK && !(rc == CURLE_WRITE_ERROR && pg.over)) { const char *hs = strstr(url, "://"); hs = hs ? hs + 3 : url; snprintf(err, en, "Could not load the page: %s (%.*s)", curl_easy_strerror(rc), (int)strcspn(hs, "/?#"), hs); }
        else snprintf(err, en, "The page answered HTTP %ld", code);
        free(pg.b); curl_easy_cleanup(h); return NULL;
    }
    Links *L = calloc(1, sizeof *L); char *sb_base = strdup(eff ? eff : url);
    curl_easy_cleanup(h);
    set_base(L, sb_base);
    {   /* <base href="..."> changes how relative links resolve */
        char *bt = strcasestr(pg.b, "<base ");
        if (bt) { char *hr = strcasestr(bt, "href="); char *gt = strchr(bt, '>');
            if (hr && gt && hr < gt) { hr += 5; char qc = (*hr == '"' || *hr == '\'') ? *hr++ : 0; size_t vl = qc ? strcspn(hr, (char[]){qc, '>', 0}) : strcspn(hr, " >");
                char tmp[2100], full[3400]; if (vl && vl < sizeof tmp) { memcpy(tmp, hr, vl); tmp[vl] = 0; if (resolve_url(L, tmp, full, sizeof full)) set_base(L, full); } } }
    }
    for (char *q = pg.b; *q; q++) if (q[0] == '\\' && q[1] == '/') { memmove(q, q + 1, strlen(q)); }   /* JSON-escaped slashes: https:\/\/ */
    /* pass 1: attributes that carry links */
    static const char *ATTR[] = { "href", "src", "data-url", "data-href", "data-src", "data-download", "data-file", NULL };
    for (char *q = pg.b; *q; q++) {
        if (*q != '=') continue;
        char *e = q; while (e > pg.b && isspace((unsigned char)e[-1])) e--;
        int hit = 0;
        for (int i = 0; ATTR[i] && !hit; i++) { size_t al = strlen(ATTR[i]); if ((size_t)(e - pg.b) >= al && !strncasecmp(e - al, ATTR[i], al) && (e - al == pg.b || !isalnum((unsigned char)e[-(long)al - 1]))) hit = 1; }
        if (!hit) continue;
        char *v = q + 1; while (isspace((unsigned char)*v)) v++;
        char quote = (*v == '"' || *v == '\'') ? *v++ : 0;
        size_t vl = quote ? strcspn(v, (char[]){quote, '\n', 0}) : strcspn(v, " \t\r\n>");
        if (vl && vl < 2100) { char tmp[2100]; memcpy(tmp, v, vl); tmp[vl] = 0; links_add(L, tmp); }
    }
    /* pass 2: absolute links anywhere (scripts, JSON, plain text) */
    for (char *q = pg.b; (q = strcasestr(q, "http")); q += 4) {
        if (strncasecmp(q, "http://", 7) && strncasecmp(q, "https://", 8)) continue;
        size_t vl = strcspn(q, "\"'<> \t\r\n\\)(");
        if (vl && vl < 2100) { char tmp[2100]; memcpy(tmp, q, vl); tmp[vl] = 0; links_add(L, tmp); }
        q += vl > 4 ? vl - 4 : 0;
    }
    /* pass 3: quoted relative file names in scripts / JSON ("files/a.zip") */
    for (char *q = pg.b; *q; q++) {
        if (*q != '"' && *q != '\'') continue;
        char *e = q + 1; size_t vl = strcspn(e, (char[]){*q, '\n', '<', '>', ' ', '\t', 0});
        if (e[vl] == *q && vl > 3 && vl < 600) { char tmp[620]; memcpy(tmp, e, vl); tmp[vl] = 0; if (strncasecmp(tmp, "http", 4) && is_file_ext(tmp)) links_add(L, tmp); q = e + vl; }
    }
    int nfiles = 0; for (int i = 0; i < L->n; i++) if (is_file_link(L->u[i])) nfiles++;
    int fb = !nfiles;                                                  /* no direct file links on the page: also offer links that look like download buttons */
    char title[140] = ""; { char *t0 = strcasestr(pg.b, "<title"); if (t0 && (t0 = strchr(t0, '>'))) { t0++; size_t tl = strcspn(t0, "<"); if (tl >= sizeof title) tl = sizeof title - 1; memcpy(title, t0, tl); title[tl] = 0; for (char *c = title; *c; c++) if (*c == '\n' || *c == '\r' || *c == '\t') *c = ' '; html_unescape(title); } }
    SB s = {0}; SBL(&s, "{\"ok\":true,\"page\":"); sb_str(&s, sb_base);
    sb_f(&s, ",\"truncated\":%d,\"total\":%d,\"diag\":{\"code\":%ld,\"bytes\":%zu,\"type\":", L->truncated || pg.over, L->n, code, pg.n); sb_str(&s, ctype); SBL(&s, ",\"title\":"); sb_str(&s, title);
    SBL(&s, "},\"links\":[");
    int out_f = 0, out_o = 0, first = 1;
    for (int i = 0; i < L->n; i++) {
        char nm[300]; const char *u = L->u[i];
        int isf = is_file_link(u) || (fb && likely_dl(u, sb_base));
        if (isf ? out_f >= SCAN_OUT_FILES : out_o >= SCAN_OUT_OTHER) { free(L->u[i]); continue; }
        if (isf) out_f++; else out_o++;
        size_t pe = strcspn(u, "?#"); const char *sl2 = u + pe;
        while (sl2 > u && sl2[-1] != '/') sl2--;
        size_t nl = (size_t)(u + pe - sl2); if (nl >= sizeof nm) nl = sizeof nm - 1;
        memcpy(nm, sl2, nl); nm[nl] = 0; pct_decode(nm);
        if (!nm[0]) snprintf(nm, sizeof nm, "(page)");
        if (!first) SBL(&s, ",{\"u\":"); else SBL(&s, "{\"u\":"); first = 0; sb_str(&s, u); SBL(&s, ",\"n\":"); sb_str(&s, nm);
        sb_f(&s, ",\"f\":%d}", isf); free(L->u[i]);
    }
    SBL(&s, "]}");
    free(L); free(sb_base); free(pg.b);
    return s.b;
}

/* ------------------------------------------------------- watch folders */
static void scan_watch_dir(const char *dir) {
    DIR *dp = opendir(dir); if (!dp) return;
    struct dirent *e;
    while ((e = readdir(dp))) {
        size_t nl = strlen(e->d_name);
        if (nl < 5 || strcasecmp(e->d_name + nl - 4, ".txt")) continue;
        char fp[1024]; snprintf(fp, sizeof fp, "%s/%s", dir, e->d_name);
        FILE *f = fopen(fp, "r"); if (!f) continue;
        char line[2300]; int added = 0;
        while (fgets(line, sizeof line, f)) {
            line[strcspn(line, "\r\n")] = 0; char *u = line; while (isspace((unsigned char)*u)) u++;
            if (!*u || *u == '#') continue;
            char *bar = strchr(u, '|'); char *nm = NULL; if (bar) { *bar = 0; nm = bar + 1; }
            char err[160]; if (eng_add(u, nm, NULL, 1, err, sizeof err, NULL) > 0) added++;
        }
        fclose(f);
        char done[1100]; snprintf(done, sizeof done, "%s.done", fp);
        if (rename(fp, done)) { if (truncate(fp, 0)) {} }
        if (added) { log_msg("[watch] %d link(s) from %s\n", added, fp); plat_notify("Download Manager: added %d link(s)", added); }
    }
    closedir(dp);
}
static void scan_watch(void) {
    char p[600]; snprintf(p, sizeof p, "%s/watch", dl_base()); scan_watch_dir(p);
    for (int i = 0; i < 8; i++) { snprintf(p, sizeof p, "/mnt/usb%d/DownloadMGR", i); scan_watch_dir(p); }
}

/* --------------------------------------------------------------- scheduler */
static void *sched_main(void *arg) {
    (void)arg; int64_t last_speed = now_ms(), last_save = last_speed; long tick = 0;
    while (!quit_flag) {
        usleep(250000); tick++;
        int64_t t = now_ms();
        pthread_mutex_lock(&L);
        char fmsg[4][160]; int nfm = 0;
        for (int qi = 0; qi < MAX_Q; qi++) {
            Queue *q = &qs[qi];
            if (q->used) {                                                 /* queue went from busy to idle because something finished: announce it and start the chained queue */
                int busy = 0; for (int i = 0; i < MAX_DL; i++) if (dls[i].used && dls[i].qid == q->id && (dls[i].state == ST_ACTIVE || dls[i].state == ST_QUEUED || dls[i].running)) { busy = 1; break; }
                if (q->was_busy && !busy && q->fin_pending) {
                    int nd = 0, ne = 0; for (int i = 0; i < MAX_DL; i++) if (dls[i].used && dls[i].qid == q->id) { if (dls[i].state == ST_DONE) nd++; else if (dls[i].state == ST_ERROR) ne++; }
                    if (S.notify >= 1 && nfm < 4) snprintf(fmsg[nfm++], sizeof fmsg[0], "Queue \"%.60s\" finished: %d done, %d failed", q->name, nd, ne);
                    Queue *nq = q->chain > 0 && q->chain != q->id ? qfind(q->chain) : NULL;
                    if (nq && !nq->running) { nq->running = 1; nq->last_in = -1; dirty = 1; log_msg("[chain] queue %d -> queue %d started\n", q->id, nq->id); }
                }
                if (!busy) q->fin_pending = 0;
                q->was_busy = busy;
            }
            if (q->used && q->sched_on) {                                  /* timer: act only when the window is entered/left, so manual Start/Stop still works in between */
                time_t nowt = time(NULL) - (time_t)q->tz * 60; struct tm tmv; gmtime_r(&nowt, &tmv);
                int m = tmv.tm_hour * 60 + tmv.tm_min;
                int in = q->start_min == q->stop_min ? 1 : q->start_min < q->stop_min ? (m >= q->start_min && m < q->stop_min) : (m >= q->start_min || m < q->stop_min);
                if (in != q->last_in) {
                    q->last_in = in; q->running = in; dirty = 1;
                    if (!in) for (int i = 0; i < MAX_DL; i++) if (dls[i].used && dls[i].qid == q->id && dls[i].state == ST_ACTIVE) { dls[i].stop_to = ST_QUEUED; dls[i].stop = 1; }
                    log_msg("[timer] queue %d %s\n", q->id, in ? "started" : "stopped");
                }
            }
            if (!q->used || !q->running) continue;
            int act = 0; for (int i = 0; i < MAX_DL; i++) if (dls[i].used && dls[i].qid == q->id && (dls[i].state == ST_ACTIVE || dls[i].running)) act++;
            while (act < q->max) {
                Download *best = NULL;
                for (int i = 0; i < MAX_DL; i++) {
                    Download *d = &dls[i];
                    if (d->used && d->qid == q->id && d->state == ST_QUEUED && !d->running && (!best || d->ord < best->ord)) best = d;
                }
                if (!best) break;
                start_dl(best); act++;
            }
        }
        if (t - last_speed >= 1000) {
            double dt = (t - last_speed) / 1000.0; last_speed = t;
            for (int i = 0; i < MAX_DL; i++) if (dls[i].used && dls[i].state == ST_ACTIVE) {
                Download *d = &dls[i]; int64_t cur = seg_done_sum(d);
                double inst = (cur - d->last_done) / dt; d->last_done = cur;
                d->speed = d->speed <= 0 ? inst : d->speed * 0.6 + inst * 0.4;
            }
        }
        if ((dirty && t - last_save >= 1000) || t - last_save >= 5000) { save_state(); dirty = 0; last_save = t; }
        pthread_mutex_unlock(&L);
        for (int k = 0; k < nfm; k++) plat_notify("%s", fmsg[k]);
        if (tick % 12 == 0) scan_watch();
    }
    return NULL;
}

/* ---------------------------------------------------------------- lifecycle */
void eng_init(void) {
    char p[600];
    S.segments = 4; S.limit_kbps = 0; S.retries = 5; S.auto_install_app = 1; S.notify = 2;
    snprintf(S.dir, sizeof S.dir, "%s", dl_base());
    mkdir_p(dl_base()); snprintf(p, sizeof p, "%s/watch", dl_base()); mkdir_p(p);
    curl_global_init(CURL_GLOBAL_DEFAULT);
    load_state();
    if (!qfind(1)) { qs[0].used = 1; qs[0].id = 1; qs[0].running = 1; qs[0].max = 2; qs[0].last_in = -1; ssafe(qs[0].name, sizeof qs[0].name, "Main Queue"); }
    path_of(p, sizeof p, "token"); FILE *f = fopen(p, "r");
    if (f) { if (!fgets(token, sizeof token, f)) token[0] = 0; fclose(f); token[strcspn(token, "\r\n")] = 0; }
    if (strlen(token) < 16) {
        FILE *r = fopen("/dev/urandom", "r"); unsigned char b[12] = {0}; if (r) { if (fread(b, 1, sizeof b, r)) {} fclose(r); } else srand((unsigned)time(NULL));
        for (int i = 0; i < 12; i++) snprintf(token + i * 2, 3, "%02x", r ? b[i] : (unsigned)(rand() & 255));
        if ((f = fopen(p, "w"))) { fputs(token, f); fclose(f); }
    }
    pthread_create(&sched_thr, NULL, sched_main, NULL);
}

void eng_shutdown(void) {
    quit_flag = 1; pthread_join(sched_thr, NULL);
    pthread_mutex_lock(&L);
    for (int i = 0; i < MAX_DL; i++) if (dls[i].used && dls[i].running) { dls[i].stop_to = ST_QUEUED; dls[i].stop = 1; }
    pthread_mutex_unlock(&L);
    for (int w = 0; w < 50; w++) {
        int any = 0; pthread_mutex_lock(&L);
        for (int i = 0; i < MAX_DL; i++) if (dls[i].used && dls[i].running) any = 1;
        pthread_mutex_unlock(&L); if (!any) break; usleep(100000);
    }
    pthread_mutex_lock(&L); save_state(); pthread_mutex_unlock(&L);
}
