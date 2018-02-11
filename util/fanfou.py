import struct
import base64
from mcrypt import MCRYPT

MESSAGE_ENCRYPT_IV = 'sAn_kUaI'
MESSAGE_ENCRYPT_KEY = 'WABJ4AM2'

def encryptXtea(key, iv, str):
	m = MCRYPT('xtea', 'ecb')
	key = key.ljust(m.get_key_size(), "\0")
	iv = iv.ljust(m.get_iv_size(), "\0")
	m.init(key, iv)
	return m.encrypt(str)

def decryptXtea(key, iv, str):
	m = MCRYPT('xtea', 'ecb')
	key = key.ljust(m.get_key_size(), "\0")
	iv = iv.ljust(m.get_iv_size(), "\0")
	m.init(key, iv)
	return m.decrypt(str)

def urlBase64Encode(str):
	encoded = base64.b64encode(str)
	return encoded.replace("+","-").replace("/","_").replace("=","")  

def urlBase64Decode(str):
	str=str.replace("-", "+").replace("_", "/")
	nfill = (4 - len(str) % 4) % 4
	# nfill = 3 - len(str) % 4
	while nfill>0:
		str += "="
		nfill -= 1
	return base64.b64decode(str)

def getEncodedMsgID(id):
	if id > 0:
		return urlBase64Encode(encryptXtea(MESSAGE_ENCRYPT_KEY, MESSAGE_ENCRYPT_IV, struct.pack('I', id)))
	else:
		return None

def getDecodedMsgID(code):
	if code != '':
		binaryStr = decryptXtea(MESSAGE_ENCRYPT_KEY, MESSAGE_ENCRYPT_IV, urlBase64Decode(code))
		return struct.unpack('I', binaryStr[:4])[0]
	else:
		return 0

if __name__ == '__main__':

	code = '5l6Vfpy02wA'
	id = 28901768

	print getDecodedMsgID('-N3Xz7duZww')
	print getEncodedMsgID(getDecodedMsgID(code))
