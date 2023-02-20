.PHONY: all
all: test

INSTALL_PREFIX ?= ${HOME}/.local/helpers
BUILD_PROXY_IO ?= ON
WITH_COVERAGE  ?= OFF
# Build unit and integration tests
WITH_TESTS     ?= ON

# Build with Ceph storge helper by default
WITH_CEPH    		?= ON
# Build with Swift storage helper by default
WITH_SWIFT   		?= ON
# Build with S3 storage helper by default
WITH_S3      		?= ON
# Build with GlusterFS storage helper by default
WITH_GLUSTERFS		?= ON
# Build with WebDAV storage helper by default
WITH_WEBDAV 		?= ON
# Build with XRootD storage helper by default
WITH_XROOTD 		?= ON
# Build with NFS storage helper by default
WITH_NFS    		?= ON


# Detect compilation on CentOS using Software Collections environment
ifeq ($(shell awk -F= '/^ID=/{print $$2}' /etc/os-release), "centos")
		OPENSSL_ROOT_DIR ?= /opt/onedata/onedata2102/root/usr
		TBB_INSTALL_DIR ?= /opt/onedata/onedata2102/root/usr
endif

%/CMakeCache.txt: **/CMakeLists.txt test/integration/* test/integration/**/*
	mkdir -p $*
	cd $* && cmake -GNinja -DCMAKE_BUILD_TYPE=$* \
	                       -DCODE_COVERAGE=${WITH_COVERAGE} \
	                       -DCMAKE_INSTALL_PREFIX=${INSTALL_PREFIX} \
	                       -DBUILD_PROXY_IO=${BUILD_PROXY_IO} \
	                       -DWITH_CEPH=${WITH_CEPH} \
	                       -DWITH_SWIFT=${WITH_SWIFT} \
	                       -DWITH_S3=${WITH_S3} \
	                       -DWITH_GLUSTERFS=${WITH_GLUSTERFS} \
	                       -DWITH_WEBDAV=${WITH_WEBDAV} \
	                       -DWITH_XROOTD=${WITH_XROOTD} \
	                       -DWITH_NFS=${WITH_NFS} \
	                       -DWITH_TESTS=${WITH_TESTS} \
	                       -DTBB_INSTALL_DIR=${TBB_INSTALL_DIR} \
	                       -DOPENSSL_ROOT_DIR=${OPENSSL_ROOT_DIR} ..
	touch $@

##
## Submodules
##

submodules:
	git submodule sync --recursive ${submodule}
	git submodule update --init --recursive ${submodule}


.PHONY: release
release: release/CMakeCache.txt
	cmake --build release --target helpersStatic
	cmake --build release --target helpersShared

.PHONY: test-release
test-release: release/CMakeCache.txt
	cmake --build release

.PHONY: debug
debug: debug/CMakeCache.txt
	cmake --build debug --target helpersStatic
	cmake --build debug --target helpersShared

.PHONY: test
test: debug
	cmake --build debug
	cmake --build debug --target test

.PHONY: cunit
cunit: debug
	cmake --build debug
	cmake --build debug --target cunit

.PHONY: install
install: release
	cmake --build release --target install

.PHONY: coverage
coverage:
	lcov --base-directory `pwd`/debug --directory `pwd`/debug --capture --output-file `pwd`/helpers.info
	lcov --base-directory `pwd`/debug --remove `pwd`/helpers.info 'test/*' '/usr/*' 'asio/*' '**/messages/*' \
	                           'relwithdebinfo/*' 'debug/*' 'release/*' \
	                           'erlang-tls/*' \
														 --output-file `pwd`/helpers.info.cleaned
	genhtml -o `pwd`/coverage `pwd`/helpers.info.cleaned
	echo "Coverage written to `pwd`/coverage/index.html"

.PHONY: clean
clean:
	rm -rf debug release

.PHONY: clang-tidy
clang-tidy:
	cmake --build debug --target clang-tidy

.PHONY: clang-format
clang-format:
	docker run --rm -e CHOWNUID=${UID} -v ${PWD}:/root/sources onedata/clang-format-check:1.3
