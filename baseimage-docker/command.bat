docker build --tag=ubuntu_dev:latest .

# Mount voi thu muc hien tai
docker run -it --mount src="$(pwd)/challenge-msql",target=/challenge-msql,type=bind ubuntu_dev 

# Cai dat mot so phan mem
apt install nano
apt-get -y install git curl vim
python3 -m pip install pylint isort autoflake yapf pytest mysql-connector tabulate pre-commit
apt-get install mysql-client -y

# Cai dat MYSQLdb
apt install libmysqlclient-dev -y
python3 -m pip install mysqlclient

# Git pull with Token (dung thay cho password)
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

# Clean code truoc khi commit (Automate Clean Code and Linting in Python)
# link: https://devsday.ru/blog/details/57746
pre-commit sample-config
git config --unset-all core.hooksPath
pre-commit install --> pre-commit installed at .git/hooks/pre-commit
nano .pre-commit-config.yaml




