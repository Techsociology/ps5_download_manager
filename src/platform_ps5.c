/* PS5 platform layer: notifications, process hygiene and the Media-tab app shortcut.
 * Built with the ps5-payload-sdk toolchain. */
#include <errno.h>
#include <ifaddrs.h>
#include <netdb.h>
#include <netinet/in.h>
#include <signal.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <sys/sysctl.h>
#include <sys/types.h>
#include <sys/user.h>
#include <unistd.h>
#include <ps5/kernel.h>
#include "dlmgr.h"

#define INCASSET(name, file)                                                   \
  __asm__(".section .rodata\n"                                                 \
          ".global " #name "\n.global " #name "_end\n.global " #name "_size\n" \
          ".align 16\n" #name ":\n"                                            \
          ".incbin \"" file "\"\n" #name "_end:\n" #name "_size:\n"            \
          ".quad " #name "_end - " #name "\n"                                  \
          ".previous\n");                                                      \
  extern const uint8_t name[];                                                 \
  extern const size_t name##_size;

INCASSET(param_json, "assets/param.json");
INCASSET(icon0_png, "assets/icon0.png");

typedef struct { char useless1[45]; char message[3075]; } notify_request_t;
int sceKernelSendNotificationRequest(int, notify_request_t *, size_t, int);
int sceNetCtlInit(void);
int sceUserServiceInitialize(void *);
int sceAppInstUtilInitialize(void);
int sceAppInstUtilTerminate(void);
int sceAppInstUtilAppInstallAll(void *);

void plat_init(void) {
    syscall(SYS_thr_set_name, -1, "dlmgr.elf");
    signal(SIGPIPE, SIG_IGN); signal(SIGHUP, SIG_IGN); signal(SIGTERM, SIG_IGN);
    sceNetCtlInit();
    int prio = 256; sceUserServiceInitialize(&prio);
}

void plat_notify(const char *fmt, ...) {
    notify_request_t r; va_list a;
    memset(&r, 0, sizeof r);
    va_start(a, fmt); vsnprintf(r.message, sizeof r.message, fmt, a); va_end(a);
    sceKernelSendNotificationRequest(0, &r, sizeof r, 0);
    log_msg("[notify] %s\n", r.message);
}

int plat_local_ip(char *buf, size_t n) {
    struct ifaddrs *ifa, *p;
    if (getifaddrs(&ifa)) return -1;
    for (p = ifa; p; p = p->ifa_next) {
        if (!p->ifa_addr || p->ifa_addr->sa_family != AF_INET || !strncmp(p->ifa_name, "lo", 2)) continue;
        if (!getnameinfo(p->ifa_addr, sizeof(struct sockaddr_in), buf, n, NULL, 0, NI_NUMERICHOST)) {
            freeifaddrs(ifa); return 0;
        }
    }
    freeifaddrs(ifa); return -1;
}

/* If an older dlmgr.elf is still running (re-sent payload), kill it so port 5837 is free. */
void plat_kill_previous_instance(void) {
    for (int tries = 0; tries < 5; tries++) {
        int mib[4] = {CTL_KERN, KERN_PROC, KERN_PROC_PROC, 0};
        size_t len = 0; pid_t victim = 0;
        if (sysctl(mib, 4, NULL, &len, NULL, 0) || !len) return;
        char *buf = malloc(len);
        if (!buf || sysctl(mib, 4, buf, &len, NULL, 0)) { free(buf); return; }
        for (size_t off = 0; off < len;) {
            struct kinfo_proc *kp = (struct kinfo_proc *)(buf + off);
            if (!kp->ki_structsize) break;
            if (!strcmp(kp->ki_comm, "dlmgr.elf") && kp->ki_pid != getpid()) { victim = kp->ki_pid; break; }
            off += kp->ki_structsize;
        }
        free(buf);
        if (!victim) return;
        kill(victim, SIGKILL); sleep(1);
    }
}

/* ---- Media-tab shortcut: writes sce_sys/param.json + icon0.png and registers the title ---- */
static int mkdir_p(const char *path) {
    char t[256]; snprintf(t, sizeof t, "%s", path);
    for (char *p = t + 1; *p; p++) if (*p == '/') { *p = 0; if (mkdir(t, 0755) && errno != EEXIST) return -1; *p = '/'; }
    return (mkdir(t, 0755) && errno != EEXIST) ? -1 : 0;
}
static int same_file(const char *path, const uint8_t *d, size_t n) {
    struct stat st; if (stat(path, &st) || (size_t)st.st_size != n) return 0;
    FILE *f = fopen(path, "r"); if (!f) return 0;
    uint8_t *b = malloc(n); int ok = b && fread(b, 1, n, f) == n && !memcmp(b, d, n);
    free(b); fclose(f); return ok;
}
static int put_file(const char *path, const uint8_t *d, size_t n) {
    FILE *f = fopen(path, "w"); if (!f) return -1;
    int ok = fwrite(d, n, 1, f) == 1; fclose(f); return ok ? 0 : -1;
}

int plat_install_app(void) {
    const char *dir = "/user/app/" DL_TITLE_ID "/sce_sys";
    const char *pp = "/user/app/" DL_TITLE_ID "/sce_sys/param.json";
    const char *ip = "/user/app/" DL_TITLE_ID "/sce_sys/icon0.png";
    if (same_file(pp, param_json, param_json_size) && same_file(ip, icon0_png, icon0_png_size)) return 0;

    plat_notify("Installing Download Manager shortcut...");
    if (mkdir_p(dir) || put_file(pp, param_json, param_json_size) || put_file(ip, icon0_png, icon0_png_size)) {
        log_msg("[app] could not write sce_sys files (errno %d)\n", errno); return -1;
    }
    int err = sceAppInstUtilInitialize();
    if (err) { log_msg("[app] AppInstUtil init 0x%08X\n", err); return -1; }

    /* Prefer the single-title installer (NID Wudg3Xe3heE); fall back to install-all. */
    int (*install_dir)(const char *, const char *, void *) = 0;
    uint32_t h;
    if (!kernel_dynlib_handle(-1, "libSceAppInstUtil.sprx", &h))
        install_dir = (void *)kernel_dynlib_resolve(-1, h, "Wudg3Xe3heE");
    err = install_dir ? install_dir(DL_TITLE_ID, "/user/app/", 0) : sceAppInstUtilAppInstallAll(0);
    sceAppInstUtilTerminate();
    if (err) { log_msg("[app] install error 0x%08X\n", err); return -1; }
    plat_notify("Download Manager shortcut ready");
    return 0;
}
