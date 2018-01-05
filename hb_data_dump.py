import psycopg2, csv
 
con = None

# try to connect
try:
    # adapter to connect to postgres db 
    con = psycopg2.connect(database='hsbd', user='nat') 
    # allows python code to execute sql commands
    cur = con.cursor()
    # execute method that process sql commands in db
    cur.execute('''DROP TABLE IF EXISTS all_contacts_hb''')

    cur.execute('''CREATE TABLE all_contacts_hb
        (   id serial PRIMARY KEY,
            contact_id integer,
            first_name text,
            last_name text,
            last_email_name text,
            emails_opened text,
            lead_status text,
            total_revenue text,
            postal_code text,
            contact_name text,
            klout_score text,
            ip_city text,
            recent_deal_close_date text,
            number_of_pageviews text,
            became_a_marketing_qualified_lead_date text,
            last_meeting_booked_campaign text,
            time_of_last_visit text,
            time_of_first_visit text,
            close_date text,
            message_2 text,
            associated_deals text,
            recent_deal_amount text,
            number_of_times_contacted text,
            number_of_sales_activities text,
            first_conversion_date text,
            recent_sales_email_clicked_date text,
            recent_sales_email_opened_date text,
            original_source text,
            education text,
            media_channel text,
            phone text,
            first_deal_created_date text,
            number_of_form_submissions text,
            currently_in_workflow text,
            last_meeting_booked_medium text,
            create_date text,
            linkedin_bio text,
            first_conversion text,
            became_a_sales_qualified_lead_date text,
            last_meeting_booked text,
            first_email_click_date text,
            track text,
            city text,
            name_1 text,
            mobile_phone_number text,
            number_of_event_completions text,
            average_pageviews text,
            emails_delivered text,
            ip_address text,
            number_of_unique_forms_submitted text,
            last_modified_date text,
            number_of_visits text,
            ip_country_code text,
            phone_number text,
            your_message text,
            became_a_subscriber_date text,
            email_confirmation_status text,
            hubspot_owner text,
            event_revenue text,
            last_activity_date text,
            next_activity_date text,
            last_meeting_booked_source text,
            owner_assigned_date text,
            state_region text,
            became_an_opportunity_date text,
            emails_clicked text,
            school text,
            follower_count text,
            message_3 text,
            last_email_open_date text,
            opted_out_of_all_email text,
            last_page_seen text,
            first_page_seen text,
            original_source_drill_down_1 text,
            last_email_send_date text,
            ip_state_code_region_code text,
            recent_conversion_date text,
            became_an_other_lifecycle_date text,
            original_source_drill_down_2 text,
            lifecycle_stage text,
            skype_name text,
            last_contacted text,
            street_address text,
            recent_conversion text,
            hubspot_team text,
            twitter_bio text,
            likelihood_to_close text,
            motivation text,
            country text,
            linkedin_connections text,
            last_email_click_date text,
            persona text,
            salutation text,
            sends_since_last_engagement text,
            became_a_customer_date text,
            currently_in_sequence text,
            ip_state_region text,
            time_last_seen text,
            time_first_seen text,
            opted_out_of_email_marketing_information text,
            became_a_lead_date text,
            recent_sales_email_replied_date text,
            website_url text,
            job_title text, 
            hubspot_score text,
            twktter_username text,
            first_referring_site text,
            last_referring_site text,
            twitter_profile_photo text,
            became_an_evangelist_date text,
            days_to_close text,
            email text,
            company_name text,
            annual_revenue text,
            first_email_open_date text,
            ip_timezone text,
            fax_number text,
            ip_country text,
            industry text,
            contact_priority text,
            first_email_send_date text,
            coding_experience text,
            email_address_quarantined text,
            num_of_employees text,
            emails_bounced text,
            associated_company text
        );''')
    print ("Table data created successfully")
 
    reader = csv.reader(open('hb_contact_list.csv', 'r', encoding = 'ISO-8859-1'))
 
    for i, row in enumerate(reader):
        if i < 1 : continue
        print(i, row)
        cur.execute('''
            INSERT INTO "all_contacts_hb" (
                "contact_id","first_name","last_name","last_email_name","emails_opened","lead_status","total_revenue","postal_code","contact_name","klout_score","ip_city","recent_deal_close_date","number_of_pageviews","became_a_marketing_qualified_lead_date","last_meeting_booked_campaign","time_of_last_visit","time_of_first_visit","close_date","message_2","associated_deals","recent_deal_amount","number_of_times_contacted","number_of_sales_activities","first_conversion_date","recent_sales_email_clicked_date","recent_sales_email_opened_date","original_source","education","media_channel","phone","first_deal_created_date","number_of_form_submissions","currently_in_workflow","last_meeting_booked_medium","create_date","linkedin_bio","first_conversion","became_a_sales_qualified_lead_date","last_meeting_booked","first_email_click_date","track","city","name_1","mobile_phone_number","number_of_event_completions","average_pageviews","emails_delivered","ip_address","number_of_unique_forms_submitted","last_modified_date","number_of_visits","ip_country_code","phone_number","your_message","became_a_subscriber_date","email_confirmation_status","hubspot_owner","event_revenue","last_activity_date","next_activity_date","last_meeting_booked_source","owner_assigned_date","state_region","became_an_opportunity_date","emails_clicked","school","follower_count","message_3","last_email_open_date","opted_out_of_all_email","last_page_seen","first_page_seen","original_source_drill_down_1","last_email_send_date","ip_state_code_region_code","recent_conversion_date","became_an_other_lifecycle_date","original_source_drill_down_2","lifecycle_stage","skype_name","last_contacted","street_address","recent_conversion","hubspot_team","twitter_bio","likelihood_to_close","motivation","country","linkedin_connections","last_email_click_date","persona","salutation","sends_since_last_engagement","became_a_customer_date","currently_in_sequence","ip_state_region","time_last_seen","time_first_seen","opted_out_of_email_marketing_information","became_a_lead_date","recent_sales_email_replied_date","website_url","job_title","hubspot_score","twktter_username","first_referring_site","last_referring_site","twitter_profile_photo","became_an_evangelist_date","days_to_close","email","company_name","annual_revenue","first_email_open_date","ip_timezone","fax_number","ip_country","industry","contact_priority","first_email_send_date","coding_experience","email_address_quarantined","num_of_employees","emails_bounced","associated_company"
            ) values %s ''', [tuple(row)]
        )
    con.commit()

finally:
    
    if con:
        con.close()