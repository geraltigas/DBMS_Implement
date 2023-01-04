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
