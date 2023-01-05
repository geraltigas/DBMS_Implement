import socket

def main():
    print("Welcome to DBMS demo by geraltigas")
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect(('localhost', 12345))
    res = s.recv(1024)
    if 'OK' in res.decode('utf-8'):
        print("Connected to server")

    res = res.decode('utf-8').replace('OK\n', '')
    if res == '':
        res = s.recv(1024).decode('utf-8')
    print(res)
    name = "?"
    while True:
        command = input(" "+ name +" > ")+"\n"
        s.send(command.encode('utf-8'))
        if command == "exit\n": # here to exit client and close socket
            s.close()
            break
        res = s.recv(1024).decode('utf-8')
        if "AUTH OK" in res:
            name = res.split(" ")[2].strip()
        print(res,end="")

if __name__ == '__main__':
    main()

# A: AUTH root root
# B: AUTH user user
# A: show tables
# B: show tables
# A: create table temp (id int, name varchar(20))
# A: create index temp_id on temp(id)
# A: insert into temp values (1, 'a')
# A: show tables
# B: show tables
# A: drop table temp
# A: create table test (id int, name varchar(20),money float, age int)
# B: create table test2 (id2 int, name2 varchar(20),money2 float, age2 int)
# A: show tables
# B: show tables
# A: insert into test values (1, 'test', 1.1, 1)
# B: insert into test2 values (1, 'test1', 1.1, 1);insert into test2 values (3, 'test3', 3.3, 3);
# B: insert into test2 values (2, 'test2', 2.2, 2)
# A: select * from test;select * from test2
# B: select * from test2
# A: create index test_id on test(id)
# A: select id,money + age as alias from test where id = 1
# B: delete from test2 where id2 = 2
# B: insert into test2 values (4, 'test4', 4.4, 4)
# A: select * from test, test2
# A: select * from test, test2 where id = id2
# A: select id,id2,name,money+money2 as total from test, test2 where id = id2
# A: begin
# A: select * from test
# A: insert into test values (2, 'test2', 2.2, 2)
# A: select * from test
# B: select * from test
# A: rollback
# A: select * from test
# B: begin
# B: select * from test
# B: insert into test values (2, 'test2', 2.2, 2)
# B: select * from test
# A: select * from test
# B: commit
# B: select * from test
# A: show tables
# A: drop table test
# A: show tables
# A: drop table test2
# A: show tables


