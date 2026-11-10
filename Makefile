# Download Manager - PS5 payload (ELF)
SDK      ?= /opt/ps5-payload-sdk
TARGET   := $(SDK)/target
# pacbrew libraries (curl, openssl, microhttpd)
HB       ?= $(TARGET)/user/homebrew
CC       := $(SDK)/bin/prospero-clang
STRIP    := $(SDK)/bin/prospero-strip
PYTHON   ?= python3

SRCS := src/main.c src/engine.c src/server.c src/platform_ps5.c
ELF  := dlmgr.elf
GEN  := include/assets_index_html.h include/assets_cacert_pem.h

# Size: -Os + gc-sections + ICF (identical functions merged) + no unwind tables (plain C). LTO=1 additionally enables link-time
# optimisation (usually a further saving, but test the ELF on a console before shipping it).
LTO     ?= 0
CFLAGS  := -Os -Wall -ffunction-sections -fdata-sections -fno-unwind-tables -fno-asynchronous-unwind-tables -Iinclude -I$(TARGET)/include -I$(HB)/include
LDFLAGS := -Wl,--gc-sections -Wl,--icf=safe -Wl,--build-id=none -Wl,-Map=dlmgr.map
ifeq ($(LTO),1)
CFLAGS  += -flto
LDFLAGS += -flto
endif
# curl / microhttpd are static, so their own dependencies (libpsl, zstd, ...) must be linked too.
# Take them from the pkg-config files that shipped with the libraries instead of guessing.
CURL_DEPS := $(shell sed -n 's/^Libs.private: *//p' $(HB)/lib/pkgconfig/libcurl.pc 2>/dev/null)
MHD_DEPS  := $(shell sed -n 's/^Libs.private: *//p' $(HB)/lib/pkgconfig/libmicrohttpd.pc 2>/dev/null)
LIBS    := $(HB)/lib/libcurl.a $(CURL_DEPS) $(HB)/lib/libssl.a $(HB)/lib/libcrypto.a $(wildcard $(HB)/lib/libz.a) \
           $(HB)/lib/libmicrohttpd.a $(MHD_DEPS) -L$(HB)/lib -lpthread \
           -lSceNetCtl -lSceUserService -lSceSystemService -lSceAppInstUtil -lSceHttp2 -lSceSsl -lSceNet

all: $(ELF)

# the page and its translations (frontend/locales/*.json, managed with Crowdin) are merged into one file, then gzipped into the ELF
build/index.html: frontend/index.html $(wildcard frontend/locales/*.json) tools/build_frontend.py
	$(PYTHON) tools/build_frontend.py frontend/index.html $@

include/assets_index_html.h: build/index.html tools/gen_assets.py
	$(PYTHON) tools/gen_assets.py $< $@ index_html --gzip

include/assets_cacert_pem.h:
	wget -O include/cacert.pem https://curl.se/ca/cacert.pem
	$(PYTHON) tools/gen_assets.py include/cacert.pem $@ cacert_pem --strip-pem
	rm -f include/cacert.pem

$(ELF): $(GEN) $(SRCS) assets/param.json assets/icon0.png
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $(SRCS) $(LIBS)
	@ls -l $@ | awk '{print "ELF size before strip: " $$5 " bytes"}'
	$(STRIP) $@
	@ls -l $@ | awk '{print "ELF size: " $$5 " bytes"}'

size: $(ELF)
	$(PYTHON) tools/map_top.py dlmgr.map 20

clean:
	rm -rf $(ELF) dlmgr.map build include/assets_index_html.h include/assets_cacert_pem.h

.PHONY: all clean size
