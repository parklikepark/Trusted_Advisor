import openpyxl
from openpyxl import load_workbook
import duckdb
import spatial
import pyfiglet 
import time     
import os
import platform


fileName='all (6).xlsx'
lang='한글' # 한글 or 영어



from datetime import datetime
now = datetime.now()
formatted = now.strftime("%Y%m%d_%H%M%S")

system_info = platform.system()  # 'Linux', 'Windows', 'Darwin'
print(system_info)

if(system_info == 'Windows'):
    os.system("cls")
else:
    os.system("clear")

result = pyfiglet.figlet_format("Trusted Advisor") 
print(result)

time.sleep(3)

if not os.path.exists('output'):
    os.mkdir('output')

path = os.path.join('output','TA_')

currPath='./'


conn = duckdb.connect(database=':memory:',read_only=False)
conn.sql("""INSTALL spatial;
            LOAD spatial;
         """)


import requests

url = "https://raw.githubusercontent.com/parklikepark/Trusted_Advisor/main/TrustedAdvisorSummary.xlsx"
response = requests.get(url)

if response.status_code == 200:
    with open("TrustedAdvisorSummary_download.xlsx", "wb") as f:
        f.write(response.content)

conn.execute("""
              create table check_list as 
              select * from st_read('TrustedAdvisorSummary_download.xlsx',layer=?,open_options = ['HEADERS=FORCE', 'FIELD_TYPES=AUTO']);
              """,[lang])

conn.execute("""
              create table recommend(recomm varchar,
                                     account_id varchar,
                                     status varchar,
                                     Total_number_of_resources_processed integer,
                                     Number_of_resources_flagged integer,
                                     Number_of_suppressed_resources integer);
             """)			

conn.sql("""
            select Recommendations,count(*) as count 
            from check_list 
            group by Recommendations
            order by 1
         """).show()

wb=openpyxl.load_workbook(filename=currPath+fileName)
res=len(wb.sheetnames)
print('Number of sheets: ',res,'sheets')

ws_names=wb.sheetnames

for sn in ws_names:
    ws = wb[sn] ## 각 괄호를 이용하여 접근 가능하다.

    print('===-------------------------------------------------------===')
    print('Sheet 이름 [' + sn + ']')


    status=ws.cell(row=4,column=1).value
    res=status.split(sep=' ')[1]
    recomm=ws.cell(row=1,column=1).value
    account_id=ws.cell(row=2,column=1).value.split(sep=': ')[1]
    tot_num=ws.cell(row=6,column=2).value.split(sep=': ')[1]
    num_flagged=ws.cell(row=7,column=2).value.split(sep=': ')[1]
    num_suppressed=ws.cell(row=8,column=2).value.split(sep=': ')[1]
    
    conn.execute("INSERT INTO recommend VALUES (?,?,?,?,?,?)", [recomm, account_id,res,tot_num,num_flagged,num_suppressed])

    if status  in ('Status: not_available','Status: ok','상태: not_available','상태: ok'):
        #print(ws.cell(row=1,column=1).value)
        print(status)
        continue

    for x in range(1,ws.max_row+1):
        for y in range(1,ws.max_column+1):
            if(ws.cell(row=x,column=y).value is not None):
                if(x==1 and y==1):
                   status=ws.cell(row=x,column=y).value
                   results=conn.execute("""
                                        select ifnull(min(Recommendations),'Other') as count
                                        from check_list
                                        where detail = ?
                                        """,(status,)).fetchone()

                   print('권장사항 영역 ['+results[0]+']')
                   with open(path+results[0]+'_'+formatted+'_'+res+'.txt', 'a', encoding='utf-8') as file:
                       file.write('\n=================================================================================================================\n')

                with open(path+results[0]+'_'+formatted+'_'+res+'.txt', 'a', encoding='utf-8') as file:
                    file.write(ws.cell(row=x,column=y).value+' ')

                print(ws.cell(row=x,column=y).value, end=" ")
        print()
        with open(path+results[0]+'_'+formatted+'_'+res+'.txt', 'a', encoding='utf-8') as file:
            file.write('\n')

conn.sql("""
            select (select max(Recommendations) from check_list cl where cl.detail=rec.recomm) recommendations,status,count(*) as count 
            from   recommend rec
            group by Recommendations,status
            order by 1,2
         """).show()

conn.sql("""
            select rec.recomm,Recommendations,status
            from   recommend rec left join check_list cl
            on     rec.recomm = cl.detail
            where Recommendations is null
            order by 1
         """).show()

conn.sql("""
            select (select max(Recommendations) from check_list cl where cl.detail=rec.recomm) recommendations,rec.* 
            from   recommend rec
            order by 1,2
         """).show()
