import psycopg2, csv
 
con = None

# try to connect
try:
    # adapter to connect to postgres db 
    con = psycopg2.connect(database='hsbd', user='nat') 
    # allows python code to execute sql commands
    cur = con.cursor()
    # execute method that process sql commands in db
    cur.execute('''DROP TABLE IF EXISTS all_contacts_g''')

    cur.execute('''CREATE TABLE all_contacts_g
        (   DATE,
            STATUS,
            STAGE,
            REFERAL,
            PROJECTED_START_DATE,
            FIRST,
            LAST,
            GENDER,
            TERM,
            LOCATION,
            SKYPE,
            START_DATE,
            COHORT,
            EXPERIENCE,
            PROFESSION,
            MOTIVATION,
            MEDIA,
            MEDIA2,
            ELEVATE_SCHOLARSHIP,
            NOTES_LOG
        );''')
    print ("Table data created successfully")
 
    reader = csv.reader(open('devleague_applicants_data.csv', 'r', encoding = 'ISO-8859-1'))
 
    for i, row in enumerate(reader):
        if i < 1 : continue
        print(i, row)
        cur.execute('''
            INSERT INTO "all_applicants_g" (
            "DATE",
            "STATUS",
            "STAGE",
            "REFERAL",
            "PROJECTED_START_DATE",
            "FIRST",
            "LAST",
            "GENDER",
            "TERM",
            "LOCATION",
            "SKYPE",
            "START_DATE",
            "COHORT",
            "EXPERIENCE",
            "PROFESSION",
            "MOTIVATION",
            "MEDIA",
            "MEDIA2",
            "ELEVATE_SCHOLARSHIP",
            "NOTES_LOG"
            ) values %s ''', [tuple(row)]
        )
    con.commit()

finally:
    
    if con:
        con.close()