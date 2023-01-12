```
gcc csv.c -fPIC -shared -O3 -o csv.dylib -I /Users/alex/projects/sqlite-lines/sqlite
gcc vsv.c -fPIC -shared -O3 -o vsv.dylib -I /Users/alex/projects/sqlite-lines/sqlite
```

```

git clone https://github.com/asg017/sqlite-xsv.git

sudo apt-get update
sudo apt-get install gcc unzip python3-pip clang wget sqlite3 libsqlite3-dev

curl https://sh.rustup.rs -sSf | sh
source "$HOME/.cargo/env"

cargo install datafusion-cli
cargo install xsv
cargo install hyperfine

pip install sqlite-utils pandas

wget https://github.com/multiprocessio/dsq/releases/download/v0.23.0/dsq-linux-x64-v0.23.0.zip
wget https://github.com/duckdb/duckdb/releases/download/v0.6.1/duckdb_cli-linux-amd64.zip
wget https://github.com/cube2222/octosql/releases/download/v0.12.0/octosql_0.12.0_linux_amd64.tar.gz


cd sqlite-xsv
cargo build --release

cd benchmarks
make _data/totals.csv

wget https://raw.githubusercontent.com/sqlite/sqlite/master/ext/misc/csv.c
wget https://raw.githubusercontent.com/nalgeon/sqlean/main/src/sqlite3-vsv.c

gcc csv.c -fPIC -shared -O3 -o csv.so
gcc sqlite3-vsv.c -fPIC -shared -O3 -o vsv.so
```
