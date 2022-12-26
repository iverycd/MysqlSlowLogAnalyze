rd /s /q  __pycache__ build
del /f /s /q *.spec

c:\pycharmprojects\python_code\venv\Scripts\pyinstaller.exe -F --clean --noconfirm MySlowLogParse.py
copy /y db_config.ini dist
pause


rem copy /y dm_config.ini dist\mysql_mig_kingbase_%version%
rem type nul > dist\mysql_mig_kingbase_%version%\custom_table.txt

rem cd dist\mysql_mig_kingbase_%version%
rem ren mysql_mig_kingbase_%version%.exe mysql_mig_kingbase.exe
rem cd ..

rem "C:\Program Files\WinRAR\Rar.exe" a -r -s -m1 C:\PycharmProjects\python_code\mysql_mig_kingbase\mysql_mig_kingbase_%version%_win.rar mysql_mig_kingbase_%version% ^

