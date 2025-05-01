.PHONY: all aw-server aw-webui build install package set-version test test-coverage test-coverage-tarpaulin test-coverage-grcov coverage coverage-html coverage-lcov

all: build
build: aw-server aw-sync

DESTDIR :=
ifeq ($(SUDO_USER),)
    PREFIX := $(HOME)/.local
else
    PREFIX := /usr/local
endif


# Build in release mode by default, unless RELEASE=false
ifeq ($(RELEASE), false)
	cargoflag :=
	targetdir := debug
else
	cargoflag := --release
	targetdir := release
endif

aw-server: set-version aw-webui
	cargo build $(cargoflag) --bin aw-server

run: aw-server
	@fuser -k 5600/tcp 2>/dev/null || true
	./target/$(targetdir)/aw-server

ui-test:
	@fuser -k 5600/tcp 2>/dev/null || true
	if ! lsof -i:5600 | grep LISTEN > /dev/null; then \
	  echo "[ui-test] Starting aw-server in background (port 5600)..."; \
	  cargo build $(cargoflag) --bin aw-server; \
	  nohup ./target/$(targetdir)/aw-server > aw-server.log 2>&1 & \
	  sleep 5; \
	fi
	@if [ ! -d aw-webui/node_modules ]; then \
	  echo "[ui-test] Installing npm dependencies..."; \
	  cd aw-webui && npm install; \
	fi
	@if ! lsof -i:27180 | grep LISTEN > /dev/null; then \
	  echo "[ui-test] Starting aw-webui dev server in background (port 27180)..."; \
	  cd aw-webui && nohup npm run serve > ../webui-serve.log 2>&1 & \
	  sleep 10; \
	fi
	cd aw-webui && npx cypress run

aw-sync: set-version
	cargo build $(cargoflag) --bin aw-sync

aw-webui:
ifeq ($(SKIP_WEBUI),true) # Skip building webui if SKIP_WEBUI is true
	@echo "Skipping building webui"
else
	make -C ./aw-webui build
endif

android:
	./install-ndk.sh
	./compile-android.sh

.PHONY: inbox-test
inbox-test:
	@fuser -k 5600/tcp 2>/dev/null || true
	if ! lsof -i:5600 | grep LISTEN > /dev/null; then \
	  echo "[inbox-test] Starting inbox service in background (port 5600)..."; \
	  make build; \
	  # [已禁用] nohup env ROCKET_CONFIG=aw-inbox-rust/Rocket.toml ./target/$(targetdir)/aw-inbox-rust > inbox.log 2>&1 & \
	  sleep 10; \
	fi
	@echo "[inbox-test] # [已禁用] Testing connectivity to http://localhost:5600 ..."
	@# [已禁用] if curl -sSf http://localhost:5600/inbox > /dev/null; then \
	  echo "[inbox-test] SUCCESS: Inbox service is reachable on port 5600."; \
	else \
	  echo "[inbox-test] ERROR: Inbox service is NOT reachable on port 5600."; \
	  exit 1; \
	fi

test:
	cargo test

fix:
	cargo fmt
	cargo clippy --fix

set-version:
	@# if GITHUB_REF_TYPE is tag and GITHUB_REF_NAME is not empty, then we are building a release
	@# as such, we then need to set the Cargo.toml version to the tag name (with leading 'v' stripped)
	@# if tag is on Python-format (short pre-release suffixes), then we need to convert it to Rust-format (long pre-release suffixes)
	@# Example: v0.12.0b3 should become 0.12.0-beta.3
	@# Can't use sed with `-i` on macOS due to: https://stackoverflow.com/a/4247319/965332
	@if [ "$(GITHUB_REF_TYPE)" = "tag" ] && [ -n "$(GITHUB_REF_NAME)" ]; then \
		VERSION_SEMVER=$(shell echo $(GITHUB_REF_NAME:v%=%) | sed -E 's/([0-9]+)\.([0-9]+)\.([0-9]+)-?(a|alpha|b|beta|rc)([0-9]+)/\1.\2.\3-\4.\5/; s/-b(.[0-9]+)/-beta\1/; s/-a(.[0-9+])/-alpha\1/'); \
		echo "Building release $(GITHUB_REF_NAME) ($$VERSION_SEMVER), setting version in Cargo.toml"; \
	    perl -i -pe "s/^version = .*/version = \"$$VERSION_SEMVER\"/" aw-server/Cargo.toml; \
	fi

test-coverage-grcov:
ifndef COVERAGE_CACHE
	# We need to remove build files in case a non-coverage test has been run
	# before without RUST/CARGO flags needed for coverage
	rm -rf target/debug
endif
	rm -rf **/*.profraw
	# Build and test
	env RUSTFLAGS="-C instrument-coverage -C link-dead-code -C opt-level=0" \
	    LLVM_PROFILE_FILE=".cov/grcov-%p-%m.profraw" \
	    cargo test --verbose

coverage-tarpaulin-html:
	cargo tarpaulin -o html --output-dir coverage-html

GRCOV_PARAMS=$(shell find .cov -name "grcov-*.profraw" -print) --binary-path=./target/debug/aw-server -s . --llvm --branch --ignore-not-existing

coverage-grcov-html: test-coverage-grcov
	grcov ${GRCOV_PARAMS} -t html -o ./target/debug/$@/
	rm -rf **/*.profraw

coverage-grcov-lcov: test-coverage-grcov
	grcov ${GRCOV_PARAMS} -t lcov -o ./target/debug/lcov.info
	rm -rf **/*.profraw

coverage: coverage-tarpaulin-html

package:
	# Clean and prepare target/package folder
	rm -rf target/package
	mkdir -p target/package
	# Copy binaries
	cp target/$(targetdir)/aw-server target/package/aw-server-rust
	cp target/$(targetdir)/aw-sync target/package/aw-sync
	# Copy service file
	cp -f aw-server.service target/package/aw-server.service
	# Copy everything into `dist/aw-server-rust`
	mkdir -p dist
	rm -rf dist/aw-server-rust
	cp -rf target/package dist/aw-server-rust

install:
	# Install aw-server and aw-sync executables
	mkdir -p $(DESTDIR)$(PREFIX)/bin/
	install -m 755 target/$(targetdir)/aw-server $(DESTDIR)$(PREFIX)/bin/aw-server
	install -m 755 target/$(targetdir)/aw-sync $(DESTDIR)$(PREFIX)/bin/aw-sync
	# Install systemd user service
	mkdir -p $(DESTDIR)$(PREFIX)/lib/systemd/user
	install -m 644 aw-server.service $(DESTDIR)$(PREFIX)/lib/systemd/user/aw-server.service

clean:
	cargo clean

build:
	RUSTFLAGS="-A unused_variables -A dead_code" cargo build --release
