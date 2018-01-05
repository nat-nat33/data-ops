import psycopg2, csv
 
con = None

# try to connect
try:
    # adapter to connect to postgres db 
    con = psycopg2.connect(database='hsbd', user='nat') 
    # allows python code to execute sql commands
    cur = con.cursor()
    # execute method that process sql commands in db
    cur.execute('''DROP TABLE IF EXISTS all_applicants_g''')

    cur.execute('''CREATE TABLE all_applicants_g
        (   id serial PRIMARY KEY,
            date text,
            status text,
            stage text,
            referal text,
            projected_start_date text,
            first text,
            last text,
            gender text,
            term text,
            location text,
            skype text,
            start_date text,
            cohort text,
            experience text,
            profession text,
            motivation text,
            media text,
            media2 text,
            elevate_scholarship text,
            notes_log text
        );''')
    print ("Table data created successfully")
 
    reader = csv.reader(open('devleague_applicants_data.csv', 'r', encoding = 'ISO-8859-1'))
 
    for i, row in enumerate(reader):
        if i < 1 : continue
        print(i, row)
        cur.execute('''
            INSERT INTO "all_applicants_g" (
            "date",
            "status",
            "stage",
            "referal",
            "projected_start_date",
            "first",
            "last",
            "gender",
            "term",
            "location",
            "skype",
            "start_date",
            "cohort",
            "experience",
            "profession",
            "motivation",
            "media",
            "media2",
            "elevate_scholarship",
            "notes_log"
            ) values %s ''', [tuple(row)]
        )
    con.commit()

finally:
    
    if con:
        con.close()