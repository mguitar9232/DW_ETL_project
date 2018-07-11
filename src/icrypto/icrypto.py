# -*- coding: utf-8 -*-


from Crypto.Cipher import AES
import base64
import hashlib
import struct

class ICrypto:
    def __init__(self):
        BLOCK_SIZE = 32
        PADDING = '|'

        fs = open("../data/secure_key", 'r')

        secret = fs.readline()
        #print(secret)
        fs.close()

        #### 32bit or 64bit check ####
        bitcheck = struct.calcsize("P") * 8

        if(bitcheck == 64):
            print('64bit MODE')
            key = hashlib.sha256(secret.encode('utf-8')).digest()
            iv = 16 * '\x00'
            mode = AES.MODE_CBC
            cipher = AES.new(key, mode, IV=iv)
        else:
            print('32bit MODE')
            cipher = AES.new(secret)

        pad = lambda s: s + (BLOCK_SIZE - len(s) % BLOCK_SIZE) * PADDING

        # 암호화 ( 암호화 -> base64 encode )
        self.encodeAES = lambda s: base64.b64encode(cipher.encrypt(pad(s)))

        # 복호화 ( base64 decode -> 복호화 )
        self.decodeAES = lambda e: cipher.decrypt(base64.b64decode(e)).rstrip(PADDING)

    # 암호화
    def encodeAES(self, data):
        encoded = self.encodeAES(data)
        return encoded

    # 복호화
    def decodeAES(self, data):
        decoded = self.decodeAES(data)
        return decoded

    def checkbit(self):
        ### 32bit or 64bit check function ####
        bitcheck = struct.calcsize("P") * 8
        return bitcheck

def input_id(flag):
    global id_file_name

    cryptoid = ICrypto()

    bitcheck = cryptoid.checkbit()

    # flag :
    #       1: bidb, 2: comdb, 3: bookdb, 4: BMGDB

    if(bitcheck == 32):
        if(flag == 1):
            id_file_name = '../data/id_bi_file_enc'
        elif(flag == 2):
            id_file_name = '../data/id_com_file_enc'
        else:
            print('[Error]Argument count is wrong. \n')
            exit(1)
    else:
        if(flag == 1):
            id_file_name = '../data/id_bi_file_enc_64'
        elif(flag == 2):
            id_file_name = '../data/id_com_file_enc_64'
        else:
            print('[Error]Argument count is wrong. \n')
            exit(1)


    idf = open(id_file_name, 'r')

    idline = idf.readline()
    #print(idline)
    idf.close()

    decode_id = cryptoid.decodeAES(idline)
    #print("decodeAES[ID]: {}".format(decode_id))
    #return(decode_id)
    return(decode_id.rstrip('\n'))

def input_pwd(flag):
    global pwd_file_name

    cryptopwd = ICrypto()

    bitcheck = cryptopwd.checkbit()

    # flag :
    #       1: bidb, 2: comdb, 3: bookdb

    if(bitcheck == 32 ):
        if(flag == 1):
            pwd_file_name = '../data/pwd_bi_file_enc'
        elif(flag == 2):
            pwd_file_name = '../data/pwd_com_file_enc'
        else:
            print('[Error]Argument count is wrong. \n')
            exit(1)
    else:
        if(flag == 1):
            pwd_file_name = '../data/pwd_bi_file_enc_64'
        elif(flag == 2):
            pwd_file_name = '../data/pwd_com_file_enc_64'
        else:
            print('[Error]Argument count is wrong. \n')
            exit(1)

    pwf = open(pwd_file_name, 'r')

    pwline = pwf.readline()
    #print(pwline)
    pwf.close()

    decode_pwd = cryptopwd.decodeAES(pwline)
    #print("decodeAES[PWD]: {}".format(decode_pwd))
    #return(decode_pwd)
    return(decode_pwd.rstrip('\n'))

