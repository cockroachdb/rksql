# Copyright 2020 The Cockroach Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied. See the License for the specific language governing
# permissions and limitations under the License. See the AUTHORS file
# for names of contributors.

GO ?= go
XGOOS :=

.PHONY: all
all: build

.PHONY: build
build:
	GOOS=$(GOOS) $(GO) build -v -o bin/distributed-semaphore github.com/cockroachdb/rksql/cmd/distributed-semaphore 
	GOOS=$(GOOS) $(GO) build -v -o bin/filesystem-simulator github.com/cockroachdb/rksql/cmd/filesystem-simulator 
	GOOS=$(GOOS) $(GO) build -v -o bin/job-coordinator github.com/cockroachdb/rksql/cmd/job-coordinator

.PHONY: clean
clean:
	rm -f bin/distributed-semaphore 
	rm -f bin/filesystem-simulator 
	rm -f bin/job-coordinator
