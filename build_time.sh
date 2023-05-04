set -e

echo "CLANG:"
time clang++ -std=c++20 -fno-omit-frame-pointer -g -Wall -pedantic -Wno-unused-function -Wno-format-security -o exe $1

echo "GCC:"
time g++ -std=c++20 -fno-omit-frame-pointer -g -Wall -pedantic -Wno-unused-function -Wno-format-security -o exe $1

