# The MIT License (MIT)
#
# Copyright (c) 2016 Harrison Harnisch <hharnisc@gmail.com> (http://hharnisc.github.io)
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.

# Original source:
# https://github.com/hharnisc/login-service/blob/master/service/integration-test/test.sh

# Blog article:
# https://hharnisc.github.io/2016/06/19/integration-testing-with-docker-compose.html

RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m'

MODE=$1
if [[ -z $MODE ]]; then
  MODE="standalone"
fi

cleanup () {
  docker-compose -f docker-compose-$MODE.yml -p ci kill
  docker-compose -f docker-compose-$MODE.yml -p ci rm -f
}
trap 'cleanup ; printf "${RED}Tests Failed For Unexpected Reasons${NC}\n"' HUP INT QUIT PIPE TERM
docker-compose -f docker-compose-$MODE.yml -p ci build && docker-compose -f docker-compose-$MODE.yml -p ci up -d --force-recreate
if [ $? -ne 0 ] ; then
  printf "${RED}Docker Compose Failed${NC}\n"
  exit -1
fi
docker logs -f ci_integration-tester_1 &
TEST_EXIT_CODE=`docker wait ci_integration-tester_1`
if [ -z ${TEST_EXIT_CODE+x} ] || [ "$TEST_EXIT_CODE" -ne 0 ] ; then
  printf "${RED}Tests Failed${NC} - Exit Code: $TEST_EXIT_CODE\n"
  printf "LOGS ================================================================\n"
  docker logs ci_georocket_${MODE}_1
  printf "=====================================================================\n"
else
  printf "${GREEN}Tests Passed${NC}\n"
fi
cleanup
exit $TEST_EXIT_CODE
