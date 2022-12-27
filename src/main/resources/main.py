# This is a sample Python script.
import socket


# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.


def main():
    print("Welcome to DBMS demo by geraltigas")
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect(('nameserver', 12345))
    res = s.recv(1024)
    if 'OK' in res.decode('utf-8'):
        print("Connected to nameserver")
    res = res.decode('utf-8').replace('OK', '')
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

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
