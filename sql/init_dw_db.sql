-- Executado pelo Postgres na primeira inicialização do container.
-- Cria o banco de dados do Data Warehouse e o usuário dedicado.

CREATE USER dw WITH PASSWORD 'dw';
CREATE DATABASE dw OWNER dw;
GRANT ALL PRIVILEGES ON DATABASE dw TO dw;
