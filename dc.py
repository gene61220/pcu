# 方法說明：
# dec(加密字串) return 解密字串 ,  
# getconfig(參數名稱 參考同路徑下之config.ini)
# getconfig_enc(參數名稱 內容解密後回傳) 
import subprocess
import configparser
cf = configparser.ConfigParser()
cf.read('D:/PRG/tool_encrypt/config_df.ini')

def dec(text64):
    return(subprocess.run(["D:/PRG/tool_encrypt/Decrypt.exe", text64 ], capture_output=True, text=True).stdout)

def getconfig(config):
    return(cf.get('BASE',config))

# def getconfig(config, configini):
#     #Config = 資料屬性名稱
#     #Configini = 檔案名稱
#     return(cf.get('BASE',config))

def getconfig_enc(config):
    return(dec(cf.get('BASE',config)))

# def getconfig_enc(config , configini):
#     #Config = 資料屬性名稱
#     #Configini = 檔案名稱
#     cf.read(configini)
#     return(dec(cf.get('BASE',config)))