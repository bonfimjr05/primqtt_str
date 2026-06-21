# testa se a porta está acessível externamente
sudo ufw status

#verifica se o firewall está bloqueando
sudo ufw allow 1883

#testa a conexão da mesma rede
nc -zv 192.168.1.8 1883

Resultado esperado: Connection to 192.168.1.8 1883 port [tcp/*] succeeded!

# para o serviço do sistema
sudo systemctl stop mosquitto
sudo pkill mosquitto


# confirma que parou
sudo ss -tlnp | grep 1883
# não deve aparecer nada agora

# sobe com sua configuração (que tem listener 0.0.0.0)
cd ~/priomqtt
mosquitto -c config/mosquitto.conf -v