#!/bin/bash
PROJECT_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." && pwd )
cd "$PROJECT_DIR"
./gradlew check
