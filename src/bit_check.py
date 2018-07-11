import struct;

result = struct.calcsize("P") * 8

if(result == 64):
        print('## 64')
else:
        print('## 32')

