# https://www.digitalocean.com/community/tutorials/how-to-install-and-manage-rabbitmq

apt-get    update
apt-get -y upgrade

echo "deb http://www.rabbitmq.com/debian/ testing main" >> /etc/apt/sources.list

curl http://www.rabbitmq.com/rabbitmq-signing-key-public.asc | sudo apt-key add -

apt-get update

sudo apt-get install rabbitmq-server

sudo rabbitmq-plugins enable rabbitmq_management

# http://[your droplet's IP]:15672/.
# The default username and password are both set “guest” for the log in.

# To start the service:
# service rabbitmq-server start

# To stop the service:
# service rabbitmq-server stop

# To restart the service:
# service rabbitmq-server restart

# To check the status:
# service rabbitmq-server status
