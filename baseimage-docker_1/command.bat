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

git commit . -m "quick fix" --no-verify (su dung de push len github)
# https://ma.ttias.be/git-commit-without-pre-commit-hook/#:~:text=Quick%20tip%20if%20you%20want,get%20a%20commit%20out%20there.&text=To%20get%20your%20commit%20through,the%20%2D%2Dno%2Dverify%20option.&text=Voila%2C%20without%20pre%2Dcommit%20hooks%20running!

# https://xuanthulab.net/cap-nhat-image-luu-image-ra-file-va-nap-image-tu-file-trong-docker.html
docker commit 3ad94481ebaf ubuntu_dev:1.0 (trong do 3ad94481ebaf la CONTAINER ID cua container name ubuntu_dev)


