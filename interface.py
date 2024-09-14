from psycopg2 import *
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import os

def dashes():
    for i in range(os.get_terminal_size().columns):
        print('-',end='')
    print()
    
def output(c):
    print()
    space=2
    data=c.fetchall()
    data=[[str(j) for j in i] for i in data]
    if len(data)==0:
        print("No output for the given query\n")
        return 
    no_of_columns=len(data[0])
    lens=[0]*no_of_columns
    column_names=[i[0] for i in c.description]
    for i in data+[column_names]:
        for j in range(len(i)):
            lens[j]=max(lens[j],len(i[j]))
    for i in range(len(lens)):
        lens[i]+=2*space
    dashes()
    for i in [column_names]:
        print('|',end='')
        for j in range(len(i)):
            lspace=(lens[j]-len(i[j]))//2
            rspace=lens[j]-len(i[j])-lspace
            for k in range(lspace):
                print(' ',end='')
            print(i[j],end='')
            for k in range(rspace):
                print(' ',end='')
            print('|',end='')
        print()
    dashes()
    for i in data:
        print('|',end='')
        for j in range(len(i)):
            lspace=(lens[j]-len(i[j]))//2
            rspace=lens[j]-len(i[j])-lspace
            for k in range(lspace):
                print(' ',end='')
            print(i[j],end='')
            for k in range(rspace):
                print(' ',end='')
            print('|',end='')
        print()
    dashes()
    print(f"Number of rows: {len(data)}")
    print()
    
def take_input(minval,a,string,tabs):
    okay_input=0
    while okay_input==0:
        try:
            b=input(f"\n"+"".join(['\t']*tabs)+f"Enter {string} number:\n"+"".join(['\t']*tabs))
            print()
            b=int(b)
            if b>=minval and b<=len(a):
                okay_input=1
            else:
                print("Please enter valid value")
        except Error:
            b=input("Please enter only integer:\n")
    return b
try:
    conn=connect(database='airportdb',user='postgres',password='123456',host='localhost',port=5432)
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    c=conn.cursor()

    tables=['aircraft',
            'airline',
            'airport',
            'airport_services',
            'baggage',
            'checkpoint',
            'crew',
            'customs',
            'employee',
            'flight',
            'maintenance',
            'passenger',
            'terminal_gate',
            'visa',
            'work_slot',
            'bookings',
            'flight_seats',
            'crew_flight_mapping',
            'employee_slot_mapping'
            ]
    while True:
        options=["\t1) Interact with tables","\t2) Run in-built queries","\t3) Enter and run custom queries"]
        print("Select option: (Enter 0 to exit)")
        for i in options:
            print(i)
        a=take_input(0,options,"option",1)
        if a==0:
            break
        elif a==1:
            print("\tWhich table do you want to work with:")
            for i in range(len(tables)):
                print(f'\t{i+1})'+tables[i])
            b=take_input(1,tables,"table index",1)
            print("\twhat do you want to do with the table? :")
            table_options=["\t1) See table data","\t2) See table structure","\t3) Insert tuple into table"]
            for i in table_options:
                print(i)
            table_option=take_input(1,table_options,"table option",1)
            if table_option==1:
                c.execute(f'select * from {tables[b-1]};')
                output(c)
            elif table_option==2:
                c.execute(f"""select column_name,data_type,character_maximum_length,column_default,is_nullable
                          from information_schema.columns
                          where table_name='{tables[b-1]}';""")
                print("\tColumns",end='')
                output(c)
                c.execute(f"""select *
                          from information_schema.tables
                          where table_name='{tables[b-1]}';""")
                print("\tTable info",end='')
                output(c)
                # c.execute(f"""
                          
                #               """)
                # print("\tPrimary key constraints")
                # output(c)
                # c.execute(f"""
                          
                #                 """)
                # print("\tUnique key constraints")
                # output(c)
                # c.execute(f"""
                          
                #                 """)
                # print("\tForeign key constraints")
                # output(c)
                print("\tConstraints",end='')
                c.execute(f"""
                                SELECT *
                                FROM information_schema.key_column_usage
                                WHERE table_name = '{tables[b-1]}'
                            """)
                output(c)
            elif table_option==3:
                if b==1:
                    aircraftid=input("\tEnter aircraft id:\n\t")
                    aircrafttype=input("\tEnter aircraft type:\n\t")
                    aircraftmanufact=input("\tEnter aircraft manufacturer:\n\t")
                    aircraftmodel=input("\tEnter aircraft model:\n\t")
                    aircraftcapacity=input("\tEnter aircraft capacity:\n\t")
                    aircraftregno=input("\tEnter aircraft regno:\n\t")
                    insert=f"insert into aircraft values({aircraftid},'{aircrafttype}','{aircraftmanufact}','{aircraftmodel}',{aircraftcapacity},'{aircraftregno}');"
                elif b==2:
                    id=input("\tEnter airline id:\n\t")
                    name=input("\tEnter airline name:\n\t")
                    iata=input("\tEnter airline iata code:\n\t")
                    icao=input("\tEnter airline icao:\n\t")
                    insert=f"insert into airline values({id},'{name}','{iata}','{icao}');"
                elif b==3:
                    id=input("\tEnter airport id:\n\t")
                    name=input("\tEnter airport name:\n\t")
                    lat=input("\tEnter airport latitude:\n\t")
                    long=input("\tEnter airport longitude:\n\t")
                    insert=f"insert into airport values({id},'{name}',{lat},{long});"
                elif b==4:
                    id=input("\tEnter service id:\n\t")
                    type=input("\tEnter service type:\n\t")
                    des=input("\tEnter service description:\n\t")
                    loc=input("\tEnter service location:\n\t")
                    cost=input("\tEnter service cost:\n\t")
                    ava=input("\tEnter service availability:\n\t")
                    insert=f"insert into airport_services values({id},'{type}','{des}','{loc}',{cost},'{ava}');"
                elif b==5:
                    id=input("\tEnter baggage id:\n\t")
                    pid=input("\tEnter passenger id:\n\t")
                    fid=input("\tEnter flight id:\n\t")
                    wei=input("\tEnter baggage weight in kg:\n\t")
                    stat=input("\tEnter baggage status:\n\t")
                    secu=input("\tEnter baggage security status:\n\t")
                    insert=f"insert into baggage values({id},{pid},{fid},{wei},'{stat}','{secu}');"
                elif b==6:
                    id=input("\tEnter checkpoint id:\n\t")
                    loc=input("\tEnter checkpoint location:\n\t")
                    cap=input("\tEnter checkpoint capacity:\n\t")
                    insert=f"insert into checkpoint values({id},'{loc}',{cap});"
                elif b==7:
                    id=input("\tEnter crew id:\n\t")
                    fname=input("\tEnter crew fname:\n\t")
                    lname=input("\tEnter crew lname:\n\t")
                    job=input("\tEnter crew job title:\n\t")
                    dob=input("\tEnter crew date of birth in format('YYYY-MM-DD'):\n\t")
                    gender=input("\tEnter crew gender:\n\t")
                    phone=input("\tEnter crew phone number:\n\t")
                    mail=input("\tEnter crew mail id:\n\t")
                    add=input("\tEnter crew address:\n\t")
                    insert=f"insert into crew values({id},'{fname}','{lname}','{job}','{dob}','{gender}','{phone}','{mail}','{add}');"
                elif b==8:
                    id=input("\tEnter customs id:\n\t")
                    pid=input("\tEnter passenger id:\n\t")
                    fid=input("\tEnter flight id:\n\t")
                    items=input("\tEnter customs items:\n\t")
                    val=input("\tEnter customs value:\n\t")
                    stat=input("\tEnter customs status:\n\t")
                    insert=f"insert into customs values({id},{pid},{fid},'{items}','{val}','{stat}');"
                elif b==9:
                    id=input("\tEnter employee id:\n\t")
                    fname=input("\tEnter employee fname:\n\t")
                    lname=input("\tEnter employee lname:\n\t")
                    job=input("\tEnter employee job title:\n\t")
                    dept=input("\tEnter employee job department:\n\t")
                    dob=input("\tEnter employee date of birth in format('YYYY-MM-DD'):\n\t")
                    gender=input("\tEnter employee gender:\n\t")
                    phone=input("\tEnter employee phone number:\n\t")
                    mail=input("\tEnter employee mail id:\n\t")
                    add=input("\tEnter employee address:\n\t")
                    insert=f"insert into employee values({id},'{fname}','{lname}','{job}','{dept}','{dob}','{gender}','{phone}','{mail}','{add}');"
                elif b==10:
                    id=input("\tEnter flight id:\n\t")
                    no=input("\tEnter flight number:\n\t")
                    daid=input("\tEnter flight departure airport id:\n\t")
                    dt=input("\tEnter flight departure terminal:\n\t")
                    dg=input("\tEnter flight departure gate:\n\t")
                    aaid=input("\tEnter flight arrival airport id:\n\t")
                    at=input("\tEnter flight arrival terminal:\n\t")
                    ag=input("\tEnter flight arrival gate:\n\t")
                    ddt=input("\tEnter flight departure date and time in format ('YYYY-MM-DD hh:mm:ss'):\n\t")
                    adt=input("\tEnter flight arrival date and time in format ('YYYY-MM-DD hh:mm:ss'):\n\t")
                    facraftid=input("\tEnter flight aircraft id:\n\t")
                    falineid=input("\tEnter flight airline id:\n\t")
                    insert=f"insert into flight values({id},'{no}',{daid},{dt},'{dg}',{aaid},{at},'{ag}','{ddt}','{adt}',{facraftid},{falineid});"
                elif b==11:
                    id=input("\tEnter maintenance id:\n\t")
                    aid=input("\tEnter maintenance aircraft id:\n\t")
                    sdt=input("\tEnter maintenance start date and time in format ('YYYY-MM-DD hh:mm:ss'):\n\t")
                    edt=input("\tEnter maintenance end date and time in format ('YYYY-MM-DD hh:mm:ss'):\n\t")
                    des=input("\tEnter maintenance description:\n\t")
                    cost=input("\tEnter maintenance cost:\n\t")
                    insert=f"insert into maintenance values({id},{aid},'{sdt}','{edt}','{des}',{cost});"
                elif b==12:
                    id=input("\tEnter passenger id:\n\t")
                    fname=input("\tEnter passenger fname:\n\t")
                    lname=input("\tEnter passenger lname:\n\t")
                    dob=input("\tEnter passenger date of birth in format('YYYY-MM-DD'):\n\t")
                    c=input("\tEnter passenger country:\n\t")
                    gender=input("\tEnter passenger gender:\n\t")
                    phone=input("\tEnter passenger phone number:\n\t")
                    mail=input("\tEnter passenger mail id:\n\t")
                    add=input("\tEnter passenger address:\n\t")
                    insert=f"insert into passenger values({id},'{fname}','{lname}','{dob}','{c}','{gender}','{phone}','{mail}','{add}');"
                elif b==13:
                    aportid=input("\tEnter airport id:\n\t")
                    t=input("\tEnter terminal:\n\t")
                    g=input("\tEnter gate:\n\t")
                    insert=f"insert into terminal_gate values ({aportid},{t},'{g}');"
                elif b==14:
                    id=input("\tEnter visa id:\n\t")
                    type=input("\tEnter visa type:\n\t")
                    expiry=input("\tEnter visa expiry in format 'YYYY-MM-DD':\n\t")
                    c=input("\tEnter visa for country:\n\t")
                    pid=input("\tEnter visa passenger id:\n\t")
                    insert=f"insert into visa values ({id},'{type}','{expiry}','{c}',{pid});"
                elif b==15:
                    id=input("\tEnter work slot id:\n\t")
                    d=input("\tEnter work slot day:\n\t")
                    time=input("\tEnter work slot time:\n\t")
                    insert=f"insert into work_slot values ({id},'{d}','{time}');"
                elif b==16:
                    pid=input("\tEnter passenger id:\n\t")
                    fid=input("\tEnter flight id:\n\t")
                    insert=f"insert into bookings values ({pid},{fid});"
                elif b==17:
                    sid=input("\tEnter seat number:\n\t")
                    fid=input("\tEnter flight id:\n\t")
                    st=input("\tEnter seat type:\n\t")
                    pid=input("\tEnter passenger id:\n\t")
                    insert=f"insert into flight_seats values ({sid},{fid},'{st}',{pid});"
                elif b==18:
                    cid=input("\tEnter crew id:\n\t")
                    fid=input("\tEnter flight id:\n\t")
                    insert=f"insert into crew_flight_mapping values ({cid},{fid});"
                elif b==19:
                    eid=input("\tEnter employee id:\n\t")
                    wid=input("\tEnter work_slot id:\n\t")
                    insert=f"insert into employee_slot_mapping values ({eid},{wid});"
                c.execute(insert)
                # output(c)
        elif a==2:
            print("\tWhich query would you like to execute:")
            custom_queries=[
                "\t1) Get passenger details using firstname and lastname",
                "\t2) Get flight details using flight_id",
                "\t3) Get flight start destination and end destination using flight_id",
                "\t4) Get empty seats for a flight using flight_id"
                            ]
            for i in custom_queries:
                print(i)
            b=take_input(1,custom_queries,"query",1)
            if b==1:
                fname=input('\tEnter firstname\n\t')
                lname=input('\tEnter lastname\n\t')
                c.execute(f"select * from passenger where pass_fname='{fname}' and pass_lname='{lname}'")
                output(c)
            elif b==2:
                flight_id=int(input("\tEnter valid flight_id\n\t"))
                c.execute(f"select * from flight where flight_id='{flight_id}'")
                output(c)
            elif b==3:
                flight_id=int(input("\tEnter valid flight_id\n\t"))
                c.execute(f"select a1.aport_name,a2.aport_name from flight,airport as a1,airport as a2 where flight.flight_id='{flight_id}' and a1.aport_id=flight.dep_aport_id and a2.aport_id=flight.arr_aport_id")
                output(c)
            elif b==4:
                flight_id=int(input("\tEnter valid flight_id\n\t"))
                c.execute(f"select * from flight_seats where flight_seats.flight_id={flight_id} and pass_id=-1")
                output(c)
        elif a==3:
            query=input("\tEnter your query:\n\t")
            query_status=True
            try:
                c.execute(query)
            except Error as error:
                print('Error is:',error)
                query_status=False            
            finally:
                if query_status:
                    # data=c.fetchall()
                    # for i in data:
                    #     print(i)
                    output(c)
except (Exception, DatabaseError) as error:
    print('Error is:',error)
finally:
    if conn:
        c.close()
        conn.close()