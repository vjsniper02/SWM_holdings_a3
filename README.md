SystemParam:
FTP Path- /r2dev/a4/landmak/ftppath

SECRET MANAGER:
SECRET_NAME: "/r2dev/lmk_ftp" 
* FTP Path- /r2dev/a4/landmak/ftppath
* S3 bucket for SFTP - seil-dev-sch-file-in

Name: "apidev/sf/service_account_key"

Environment Variable 
EnvPrefix

#SSM Param
Name: /r2dev/a4/landmak/ftppath 
Value: "/ftp.out/DATAEXTRACT/DATA"

Name: /r2dev/a4/landmak/secretname
Value: "r2dev/lmk_ftp"

# Environment change option between r2dev and r2sit
# Its important to choose the specific value in parameters section (In: r2_int_a4_template.yaml) for the appopriate Environment r2dev or r2sit, as by default the value is set to r2dev.