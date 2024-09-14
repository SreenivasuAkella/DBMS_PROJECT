import random
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

def abs_date(a,b,c):
        return (a-2023)*12*28+(b-1)*28+(c-1)
def true_date(i): # any integer
        return ((i//(28*12))+2023,(i//28)%12+1,(i%28)+1)
def abs_time(a,b):
        return (a*12+b//5)/1000
def true_time(i): # 0.000 to 0.287
        i*=1000
        return (int(i//12),int((i%12)*5))
def time_line(a,b):
        return a+b


tables={
    "passenger":"""create table passenger( 
    pass_id INT NOT NULL, 
    pass_fname VARCHAR(20) NOT NULL, 
    pass_lname VARCHAR(20) NOT NULL, 
    pass_dob DATE NOT NULL, 
    pass_country VARCHAR(20) NOT NULL, 
    pass_gender VARCHAR(10) NOT NULL, 
    pass_phone VARCHAR(15) NOT NULL, 
    pass_mail VARCHAR(30) NOT NULL, 
    pass_address VARCHAR(80) NOT NULL, 
    PRIMARY KEY(pass_id) );"""
    
    
    ,"airport":"""create table airport( 
    aport_id INT NOT NULL, 
    aport_name VARCHAR(20) NOT NULL, 
    aport_loc_lat DECIMAL(5,2) NOT NULL, 
    aport_loc_long DECIMAL(5,2) NOT NULL, 
    PRIMARY KEY(aport_id) );"""
    
    
    ,"airline":"""create table airline( 
    aline_id INT NOT NULL, 
    aline_name VARCHAR(20) NOT NULL, 
    iata_code VARCHAR(20) NOT NULL, 
    icao_code VARCHAR(20) NOT NULL, 
    PRIMARY KEY(aline_id) );"""
    
    
    ,"aircraft":"""create table aircraft( 
    acraft_id INT NOT NULL, 
    acraft_type VARCHAR(40) NOT NULL, 
    acraft_manufact VARCHAR(40) NOT NULL, 
    acraft_model VARCHAR(40) NOT NULL, 
    acraft_capacity INT NOT NULL, 
    acraft_regno VARCHAR(40) NOT NULL, 
    PRIMARY KEY(acraft_id) );"""
    
    
    ,"employee":"""create table employee( 
    emp_id INT NOT NULL, 
    emp_fname VARCHAR(20) NOT NULL, 
    emp_lname VARCHAR(20) NOT NULL, 
    emp_jobtitle VARCHAR(20) NOT NULL, 
    emp_dept VARCHAR(20) NOT NULL, 
    emp_dob DATE NOT NULL, 
    emp_gender VARCHAR(10) NOT NULL, 
    emp_phone VARCHAR(15) NOT NULL, 
    emp_mail VARCHAR(30) NOT NULL, 
    emp_address VARCHAR(80) NOT NULL, 
    PRIMARY KEY(emp_id) );"""
    
    
    ,"crew":"""create table crew( 
    crew_id INT NOT NULL, 
    crew_fname VARCHAR(20) NOT NULL, 
    crew_lname VARCHAR(20) NOT NULL, 
    crew_jobtitle VARCHAR(20) NOT NULL, 
    crew_dob DATE NOT NULL, 
    crew_gender VARCHAR(10) NOT NULL, 
    crew_phone VARCHAR(15) NOT NULL, 
    crew_mail VARCHAR(30) NOT NULL, 
    crew_address VARCHAR(80) NOT NULL, 
    PRIMARY KEY(crew_id) );"""
    
    
    ,"visa":"""create table visa( 
    visa_id INT NOT NULL, 
    visa_type VARCHAR(20) NOT NULL, 
    visa_expiry DATE NOT NULL, 
    visa_for_country VARCHAR(20) NOT NULL, 
    pass_id INT NOT NULL, 
    PRIMARY KEY(pass_id,visa_id), 
    CONSTRAINT Fk_visa FOREIGN KEY(pass_id) REFERENCES passenger(pass_id) );"""
    
    
    ,"terminal_gate":"""create table terminal_gate( 
    aport_id INT NOT NULL, 
    terminal INT NOT NULL, 
    gate VARCHAR(10) NOT NULL, 
    PRIMARY KEY(terminal,gate,aport_id), 
    CONSTRAINT Fk_aport_id FOREIGN KEY(aport_id) REFERENCES airport(aport_id) );"""
    
    
    ,"work_slot":"""create table work_slot( 
    slot_id INT NOT NULL, 
    slot_day VARCHAR(10) NOT NULL, 
    slot_start_time TIME NOT NULL,
    slot_end_time TIME NOT NULL,
    PRIMARY KEY(slot_id) , 
    CONSTRAINT Chk_day CHECK (slot_day in ('Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday')));"""
    
    
    ,"maintenance":"""create table maintenance( 
    mainten_id INT NOT NULL, 
    acraft_id INT NOT NULL, 
    mainten_start_datetime TIMESTAMP NOT NULL, 
    mainten_end_datetime TIMESTAMP NOT NULL, 
    mainten_descrip VARCHAR(80) NOT NULL, 
    mainten_cost DECIMAL NOT NULL, 
    PRIMARY KEY(mainten_id,acraft_id), 
    CONSTRAINT Fk_acraft_id FOREIGN KEY(acraft_id) REFERENCES aircraft(acraft_id),
    CHECK (mainten_start_datetime<=mainten_end_datetime)
    );"""
    
    
    ,"checkpoint":"""create table checkpoint( 
    check_id INT NOT NULL, 
    check_location VARCHAR(20) NOT NULL, 
    check_capacity INT NOT NULL, 
    PRIMARY KEY(check_id) );"""
    
    
    ,"flight":"""create table flight( 
    flight_id INT NOT NULL, 
    flight_no VARCHAR(20) NOT NULL, 
    dep_aport_id INT NOT NULL, 
    dep_terminal INT NOT NULL, 
    dep_gate VARCHAR(10) NOT NULL, 
    arr_aport_id INT NOT NULL, 
    arr_terminal INT NOT NULL, 
    arr_gate VARCHAR(10) NOT NULL, 
    dep_datetime TIMESTAMP NOT NULL,
    arr_datetime TIMESTAMP NOT NULL, 
    flight_acraft_id INT NOT NULL, 
    flight_aline_id INT NOT NULL, 
    PRIMARY KEY(flight_id), 
    CONSTRAINT Fk_flight_acraft_id FOREIGN KEY(flight_acraft_id) REFERENCES aircraft(acraft_id), 
    CONSTRAINT Fk_flight_aline_id FOREIGN KEY(flight_aline_id) REFERENCES airline(aline_id), 
    CONSTRAINT Fk_dep_aport_info FOREIGN KEY(dep_aport_id,dep_terminal,dep_gate) REFERENCES terminal_gate(aport_id,terminal,gate),
    CONSTRAINT Fk_arr_aport_id FOREIGN KEY(arr_aport_id,arr_terminal,arr_gate) REFERENCES terminal_gate(aport_id,terminal,gate)
    );"""
    
    ,"bookings":"""create table bookings( 
    pass_id INT NOT NULL , 
    flight_id INT NOT NULL, 
    PRIMARY KEY (pass_id , flight_id), 
    CONSTRAINT Fk_pass_id FOREIGN KEY (pass_id) REFERENCES passenger(pass_id), 
    CONSTRAINT Fk_flight_id FOREIGN KEY (flight_id) REFERENCES flight(flight_id) );"""
    
    
    ,"flight_seats":"""create table flight_seats( 
    seat_no INT NOT NULL, 
    flight_id INT NOT NULL, 
    seat_type VARCHAR(20) NOT NULL , 
    pass_id INT , 
    PRIMARY KEY ( seat_no, flight_id ) , 
    CONSTRAINT Fk_flight_id FOREIGN KEY (flight_id) REFERENCES flight(flight_id), 
    CONSTRAINT Fk_pass_id FOREIGN KEY (pass_id) REFERENCES passenger(pass_id) );"""
    
    ,"airport_services":"""create table airport_services( 
    serv_id INT NOT NULL, 
    serv_type VARCHAR(20) NOT NULL, 
    serv_descrip VARCHAR(80) NOT NULL, 
    serv_location VARCHAR(20) NOT NULL, 
    serv_cost DECIMAL NOT NULL, 
    serv_availability VARCHAR(20) NOT NULL, 
    PRIMARY KEY(serv_id) );"""
    
    
    ,"baggage":"""create table baggage( 
    bag_id INT NOT NULL, 
    pass_id INT NOT NULL, 
    flight_id INT NOT NULL, 
    bag_weight_in_kg NUMERIC NOT NULL, 
    bag_status VARCHAR(20) NOT NULL, 
    bag_sec_status VARCHAR(20) NOT NULL, 
    PRIMARY KEY(bag_id,pass_id,flight_id), 
    CONSTRAINT Fk_pass_id FOREIGN KEY(pass_id) REFERENCES passenger(pass_id), 
    CONSTRAINT Fk_flight_id FOREIGN KEY(flight_id) REFERENCES flight(flight_id) );"""
    
    
    ,"customs":"""create table customs( 
    cust_id INT NOT NULL, 
    pass_id INT NOT NULL, 
    flight_id INT NOT NULL, 
    cust_items VARCHAR(50) NOT NULL, 
    cust_value VARCHAR(20) NOT NULL, 
    cust_status VARCHAR(20) NOT NULL, 
    PRIMARY KEY(cust_id,pass_id,flight_id), 
    CONSTRAINT Fk_pass_id FOREIGN KEY(pass_id) REFERENCES passenger(pass_id), 
    CONSTRAINT Fk_flight_id FOREIGN KEY(flight_id) REFERENCES flight(flight_id) );"""
    
    ,"crew_flight_mapping":"""create table crew_flight_mapping(
    crew_id INT NOT NULL,
    flight_id INT NOT NULL,
    PRIMARY KEY(crew_id,flight_id),
    CONSTRAINT Fk_crew_id FOREIGN KEY(crew_id) REFERENCES crew(crew_id),  
    CONSTRAINT Fk_flight_id FOREIGN KEY(flight_id) REFERENCES flight(flight_id)
    );"""
    
    ,"employee_slot_mapping":"""create table employee_slot_mapping(
    emp_id INT NOT NULL,
    slot_id INT NOT NULL,
    PRIMARY KEY(emp_id,slot_id),
    CONSTRAINT Fk_emp_id FOREIGN KEY(emp_id) REFERENCES employee(emp_id),  
    CONSTRAINT Fk_slot_id FOREIGN KEY(slot_id) REFERENCES work_slot(slot_id)
    );"""
 
    ,"constraints":"""
                CREATE OR REPLACE FUNCTION check_overlapping_time_ranges() RETURNS TRIGGER AS $$
                BEGIN
                    IF TG_OP = 'INSERT' OR TG_OP = 'UPDATE' THEN
                        IF EXISTS (
                            SELECT 1
                            FROM flight tt1
                            WHERE tt1.flight_acraft_id = NEW.flight_acraft_id
                            AND (
                                (NEW.dep_datetime, NEW.arr_datetime) OVERLAPS (tt1.dep_datetime, tt1.arr_datetime)
                                OR
                                (tt1.dep_datetime, tt1.arr_datetime) OVERLAPS (NEW.dep_datetime, NEW.arr_datetime)
                            )
                        ) THEN
                            RAISE EXCEPTION 'Time range overlaps with an existing record for this flight aircraft';
                        END IF;
                    END IF;
                    RETURN NEW;
                END;
                $$ LANGUAGE plpgsql;
                CREATE TRIGGER check_overlapping_time_ranges_trigger
                BEFORE INSERT OR UPDATE ON flight
                FOR EACH ROW
                EXECUTE PROCEDURE check_overlapping_time_ranges();
                
                
                CREATE OR REPLACE FUNCTION check_overlapping_time_ranges_booking() RETURNS TRIGGER AS $$
                BEGIN
                    IF TG_OP = 'INSERT' OR TG_OP = 'UPDATE' THEN
                        IF EXISTS (
                            SELECT 1
                            FROM bookings tt1,flight f,flight f2
                            WHERE tt1.pass_id = NEW.pass_id AND NEW.flight_id= f.flight_id AND tt1.flight_id = f2.flight_id
                            AND (
                                (f.dep_datetime, f.arr_datetime) OVERLAPS (f2.dep_datetime, f2.arr_datetime)
                                OR
                                (f2.dep_datetime, f2.arr_datetime) OVERLAPS (f.dep_datetime, f.arr_datetime)
                            )
                        ) THEN
                            RAISE EXCEPTION 'Time range overlaps with an existing record for this booking';
                        END IF;
                    END IF;
                    RETURN NEW;
                END;
                $$ LANGUAGE plpgsql;
                CREATE TRIGGER check_overlapping_time_ranges_booking_trigger
                BEFORE INSERT OR UPDATE ON bookings
                FOR EACH ROW
                EXECUTE PROCEDURE check_overlapping_time_ranges_booking();
                
                
                CREATE OR REPLACE FUNCTION check_overlapping_time_ranges_crew_flight_mapping() RETURNS TRIGGER AS $$
                BEGIN
                    IF TG_OP = 'INSERT' OR TG_OP = 'UPDATE' THEN
                        IF EXISTS (
                            SELECT 1
                            FROM crew_flight_mapping tt1,flight f,flight f2
                            WHERE tt1.crew_id = NEW.crew_id AND NEW.flight_id= f.flight_id AND tt1.flight_id = f2.flight_id
                            AND (
                                (f.dep_datetime, f.arr_datetime) OVERLAPS (f2.dep_datetime, f2.arr_datetime)
                                OR
                                (f2.dep_datetime, f2.arr_datetime) OVERLAPS (f.dep_datetime, f.arr_datetime)
                            )
                        ) THEN
                            RAISE EXCEPTION 'Time range overlaps with an existing record for this crew and flight mapping';
                        END IF;
                    END IF;
                    RETURN NEW;
                END;
                $$ LANGUAGE plpgsql;
                CREATE TRIGGER check_overlapping_time_ranges_cfm_trigger
                BEFORE INSERT OR UPDATE ON crew_flight_mapping
                FOR EACH ROW
                EXECUTE PROCEDURE check_overlapping_time_ranges_crew_flight_mapping();
                
                
                CREATE OR REPLACE FUNCTION check_overlapping_time_ranges_e_s_mapping() RETURNS TRIGGER AS $$
                BEGIN
                    IF TG_OP = 'INSERT' OR TG_OP = 'UPDATE' THEN
                        IF EXISTS (
                            SELECT 1
                            FROM employee_slot_mapping tt1,work_slot f,work_slot f2
                            WHERE tt1.emp_id = NEW.emp_id AND NEW.slot_id= f.slot_id AND tt1.slot_id = f2.slot_id
                            AND (
                                (f.slot_start_time, f.slot_end_time) OVERLAPS (f2.slot_start_time, f2.slot_end_time)
                                OR
                                (f2.slot_start_time, f2.slot_end_time) OVERLAPS (f.slot_start_time, f.slot_end_time)
                            )
                        ) THEN
                            RAISE EXCEPTION 'Time range overlaps with an existing record for this crew and flight mapping';
                        END IF;
                    END IF;
                    RETURN NEW;
                END;
                $$ LANGUAGE plpgsql;
                CREATE TRIGGER check_overlapping_time_ranges_esm_trigger
                BEFORE INSERT OR UPDATE ON employee_slot_mapping
                FOR EACH ROW
                EXECUTE PROCEDURE check_overlapping_time_ranges_e_s_mapping();
                
                """
}

try:
    conn=psycopg2.connect(user='postgres',password='123456',host='localhost',port=5432)
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    c=conn.cursor()
    c.execute("drop database if exists airportdb")
    c.execute("create database airportdb")
    
except (Exception, psycopg2.DatabaseError) as error:
    print('Error is:',error)
finally:
    if conn:
        c.close()
        conn.close()
        
try:
    conn=psycopg2.connect(database='airportdb',user='postgres',password='123456',host='localhost',port=5432)
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    c=conn.cursor()
    for i in tables:
        if tables[i]!='':
            c.execute(tables[i])
    pass_ids=[]
    aport_ids=[]
    aline_ids=[]
    acraft_ids=[]
    emp_ids=[]
    crew_ids=[]
    terminal_gate=[]
    work_slot=[]
    check_ids=[]
    serv_ids=[]
    flight_times=[]
    
    for i in range(1,101):
        
        # PASSENGER
        c.execute(f"insert into passenger values({i},'Pass_Fname{random.randint(1,100):03d}','Pass_Lname{random.randint(1,100):03d}','{random.randint(1980,2005)}-{random.randint(1,12)}-{random.randint(1,28)}','Pass_Country{random.randint(1,100):03d}','Gender{random.randint(1,2)}','{(random.random()*10+90)*10e8:.0f}','{random.randint(100000,999999)}@gmail.com','Pass_Address{random.randint(1,100):03d}'  );")
        pass_ids.append(i)
        
        # AIRPORT
        if i%10==1:
            j=(i//10)+1
            c.execute(f"insert into airport values({j},'Aport_Name{j:03d}',{random.randint(-9000,9000)/100:03.2f},{random.randint(-18000,18000)/100:03.2f} );")
            aport_ids.append(j)
            
        # AIRLINE
        if i%10==1:
            j=(i//10)+1
            c.execute(f"insert into airline values({j},'Aline_Name{j:03d}','IATA_Code{j:03d}','ICAO_Code{j:03d}' );")
            aline_ids.append(j)
            
            
        # AIRCRAFT
        if i%10==1:
            j=(i//10)+1
            for k in range(1,4):
                c.execute(f"insert into aircraft values({(j-1)*3+k},'Acraft_Type{j:03d}-{k}','Acraft_manufact{j:03d}','Acraft_Model{j:03d}-{k}-{random.randint(2,9)*100}',{random.randint(10,19)*20},'Acraft_Regno{(j-1)*3+k:03d}' );")
                acraft_ids.append((j-1)*3+k)
        
        # EMPLOYEE
        c.execute(f"insert into employee values({i},'Emp_Fname{random.randint(1,100):03d}','Emp_Lname{random.randint(1,100):03d}','Emp_Jobtitle{random.randint(1,10):02d}','Emp_Dept{random.randint(1,10):02d}','{random.randint(1980,2005)}-{random.randint(1,12)}-{random.randint(1,28)}','Gender{random.randint(1,2)}','{(random.random()*10+90)*10e8:.0f}','{random.randint(100000,999999)}@gmail.com','Emp_Address{random.randint(1,100):03d}'  );")
        emp_ids.append(i)
        
        # CREW
        c.execute(f"insert into crew values({i},'Crew_Fname{random.randint(1,100):03d}','Crew_Lname{random.randint(1,100):03d}','Crew_Jobtitle{random.randint(1,10):02d}','{random.randint(1980,2005)}-{random.randint(1,12)}-{random.randint(1,28)}','Gender{random.randint(1,2)}','{(random.random()*10+90)*10e8:.0f}','{random.randint(100000,999999)}@gmail.com','Crew_Address{random.randint(1,100):03d}'  );")
        crew_ids.append(i)
        
    # TERMINAL_GATE
    for i in range(1,10):
        for j in range(1,random.randint(2,3)):
            for k in range(1,random.randint(2,4)):
                c.execute(f"insert into terminal_gate values( {i},{j},'{chr(64+k)}' );")
                terminal_gate.append(tuple([i,j,chr(64+k)]))
    # WORK_SLOT
    days=['Sunday','Monday','Tuesday','Wednesday','Thursday','Friday','Saturday']
    slots={'00:00:01':'03:00:00','03:00:01':'06:00:00','06:00:01':'09:00:00','09:00:01':'12:00:00','12:00:01':'15:00:00','15:00:01':'18:00:00','18:00:01':'21:00:00','21:00:01':'00:00:00'}
    count=1
    for i in range(len(days)):
        for j in slots:
            c.execute(f"insert into work_slot values({count},'{days[i]}','{j}','{slots[j]}' );")
            work_slot.append(count)
            count+=1
            
    # CHECKPOINT
    for i in range(1,10):
        c.execute(f"insert into checkpoint values({i},'Checkpoint_Location{i}',{random.randint(70,99)*10} );")
        check_ids.append(i)
        
    # AIRPORT_SERVICES
    z=0
    for i in range(1,6):
        for j in range(1,random.randint(3,6)):
            z+=1
            c.execute(f"insert into airport_services values({z},'Serv_Type{i}','Serv_Descrip{i}-{j}','Serv_Location{random.randint(1,10):03d}',{random.randint(12,20)*100},'Serv_Availability{z}' );")
            serv_ids.append(z)
    
    # VISA
    visa_pass=[]
    visa_country=[]
    for i in range(1,101):
        z=random.randint(0,len(pass_ids)-1)
        country=random.randint(1,100)
        while (i,pass_ids[z],country) in visa_pass:
            z=random.randint(0,len(pass_ids)-1)
        visa_pass.append((i,pass_ids[z]))
        while (i,country) in visa_country:
            country=random.randint(1,100)
        visa_country.append((i,country))
        
        c.execute(f"insert into visa values( {i},'Visa_Type{random.randint(1,3)}','{random.randint(2024,2030)}-{random.randint(1,12)}-{random.randint(1,28)}','Visa_For_Country{country:03d}',{pass_ids[z]} );")
        
    # FLIGHTS
      
    dep_place_time=[]
    arr_place_time=[]
    acraft_time=[]
    for id in range(1,101):
        
        dep_day=random.randint(1,600)
        dep_time=random.randint(0,287)/1000
        dep=dep_day+dep_time
        arr_day=random.randint(dep_day,dep_day+1)
        arr_time=random.randint(0,287)/1000
        arr=arr_day+arr_time
        while arr-dep<=0.048:
            arr_day=random.randint(dep_day,dep_day+1)
            if arr_day==dep_day:
                arr_time=random.randint(int(dep_time*1000),287)/1000
            else:
                arr_time=random.randint(0,287)/1000
            arr=arr_day+arr_time
        
        dep_place=terminal_gate[random.randint(0,len(terminal_gate)-1)]
        check_dep_place=0
        while check_dep_place==0:
            for i in dep_place_time:
                if i[0]==dep_place and i[1]==dep:
                    dep_place=terminal_gate[random.randint(0,len(terminal_gate)-1)]
                    break
            check_dep_place=1
        arr_place=terminal_gate[random.randint(0,len(terminal_gate)-1)]
        while arr_place==dep_place:
            arr_place=terminal_gate[random.randint(0,len(terminal_gate)-1)]
        check_arr_place=0
        while check_arr_place==0:
            for i in arr_place_time:
                if i[0]==arr_place and i[1]==arr:
                    arr_place=terminal_gate[random.randint(0,len(terminal_gate)-1)]
                    while arr_place==dep_place:
                        arr_place=terminal_gate[random.randint(0,len(terminal_gate)-1)]
                    break
            check_arr_place=1
        dep_place_time.append((dep_place,dep))
        arr_place_time.append((arr_place,arr))
        
        acraft=acraft_ids[random.randint(0,len(acraft_ids)-1)]
        check_acraft_time=0
        if len(acraft_time)>0:
            while check_acraft_time==0:
                no=0
                for i in acraft_time:
                    if i[0]==acraft and ((dep>=i[1] and dep<=i[2]) or (arr>=i[1] and arr<=i[2]) or (i[1]>=dep and i[1]<=arr) or (i[2]>=dep and i[2]<=arr)):
                        acraft=acraft_ids[random.randint(0,len(acraft_ids)-1)]
                        no=1
                        break
                if no==0:
                    check_acraft_time=1
        acraft_time.append([acraft,dep,arr])
        
        td0=int(true_date(dep_day)[0])
        td1=int(true_date(dep_day)[1])
        td2=int(true_date(dep_day)[2])
        ta0=int(true_date(arr_day)[0])
        ta1=int(true_date(arr_day)[1])
        ta2=int(true_date(arr_day)[2])
        # print(f"'{td0}-{td1}-{td2} {true_time(dep_time)[0]:02d}:{true_time(dep_time)[1]:02d}:00','{ta0}-{ta1}-{ta2} {true_time(arr_time)[0]:02d}:{true_time(arr_time)[1]:02d}:00',acraft:{acraft}")
        try:
            c.execute(f"insert into flight values( {id},'Flight_No{id}',{dep_place[0]},{dep_place[1]},'{dep_place[2]}',{arr_place[0]},{arr_place[1]},'{arr_place[2]}','{td0}-{td1:02d}-{td2:02d} {true_time(dep_time)[0]:02d}:{true_time(dep_time)[1]:02d}:00','{ta0}-{ta1:02d}-{ta2:02d} {true_time(arr_time)[0]:02d}:{true_time(arr_time)[1]:02d}:00',{acraft},{aline_ids[random.randint(0,len(aline_ids)-1)]} );")
            flight_times.append((id,dep,arr))
        except psycopg2.Error as error:
            print(error)
    # MAINTENANCE
    for i in range(1,101):
        start_day=random.randint(1,600)
        start_time=random.randint(0,287)/1000
        start=start_day+start_time
        end_day=random.randint(start_day,start_day+1)
        end_time=random.randint(0,287)/1000
        end=end_day+end_time
        while end-start<=0.024:
            end_day=random.randint(start_day,start_day+1)
            end_time=random.randint(0,287)/1000
            end=end_day+end_time
        sd0=int(true_date(start_day)[0])
        sd1=int(true_date(start_day)[1])
        sd2=int(true_date(start_day)[2])
        ed0=int(true_date(end_day)[0])
        ed1=int(true_date(end_day)[1])
        ed2=int(true_date(end_day)[2])
        c.execute(f"insert into maintenance values( {i},{acraft_ids[random.randint(0,len(acraft_ids)-1)]},'{sd0}-{sd1}-{sd2} {true_time(start_time)[0]}:{true_time(start_time)[1]}:00','{ed0}-{ed1}-{ed2} {true_time(end_time)[0]}:{true_time(end_time)[1]}:00','Mainten_Descrip{i}',{random.randint(70,99)*1000} );")
        
    # BAGGAGE
    pass_flights=[]
    for i in range(0,101):
        bag_pass=pass_ids[random.randint(0,len(pass_ids)-1)]
        bag_flight=flight_times[random.randint(0,len(flight_times)-1)]
        check_bag_flight=0
        while check_bag_flight==0:
            if (bag_pass,bag_flight) in pass_flights:
                bag_flight=flight_times[random.randint(0,len(flight_times)-1)]
                break
            for j in pass_flights:
                if j[0]==bag_pass:
                    if (bag_flight[1]>j[1][1] and bag_flight[1]<j[1][2]) or (bag_flight[2]>j[1][1] and bag_flight[2]<j[1][2]):
                        bag_flight=flight_times[random.randint(0,len(flight_times)-1)]
                        break
            check_bag_flight=1
        pass_flights.append((bag_pass,bag_flight))
        c.execute(f"insert into baggage values( {i},{bag_pass},{bag_flight[0]},'{random.randint(10,50)/10:1.1f}','Bag_Status{i:03d}','Bag_Sec_Status{i:03d}' );")
    
    # CUSTOMS
    pass_flights_customs=[]
    for i in range(0,101):
        cust_pass=pass_ids[random.randint(0,len(pass_ids)-1)]
        cust_flight=flight_times[random.randint(0,len(flight_times)-1)]
        check_cust_flight=0
        while check_cust_flight==0:
            if (cust_pass,cust_flight) in pass_flights_customs:
                cust_flight=flight_times[random.randint(0,len(flight_times)-1)]
                break
            for j in pass_flights_customs:
                if j[0]==cust_pass:
                    if (cust_flight[1]>j[1][1] and cust_flight[1]<j[1][2]) or (cust_flight[2]>j[1][1] and cust_flight[2]<j[1][2]):
                        cust_flight=flight_times[random.randint(0,len(flight_times)-1)]
                        break
            check_cust_flight=1
        pass_flights_customs.append((cust_pass,cust_flight))
        c.execute(f"insert into customs values( {i},{cust_pass},{cust_flight[0]},'Cust_Items{i:03d}','Cust_Value{i:03d}','Cust_Status{i:03d}' );")
            
        
# except (Exception, psycopg2.DatabaseError) as error:
#     print('Error is:',error)
finally:
    if conn:
        c.close()
        conn.close()