DROP TABLE IF EXISTS act CASCADE;
DROP TABLE IF EXISTS gig CASCADE;
DROP TABLE IF EXISTS act_gig CASCADE;
DROP TABLE IF EXISTS venue CASCADE;
DROP TABLE IF EXISTS gig_ticket CASCADE;
DROP TABLE IF EXISTS ticket CASCADE;

CREATE TABLE act(
    actID SERIAL NOT NULL PRIMARY KEY,
    actname VARCHAR(100) NOT NULL,
    genre VARCHAR(20) NOT NULL,
    members INTEGER NOT NULL CHECK (members > 0),
    standardfee INTEGER NOT NULL CHECK (standardfee >= 0)
);

CREATE TABLE gig (
    gigID SERIAL NOT NULL PRIMARY KEY,
    venueid INTEGER NOT NULL,
    gigtitle VARCHAR(100) NOT NULL,
    gigdate TIMESTAMP NOT NULL,
    gigstatus VARCHAR(10) NOT NULL
);

-- Notice that here is no primary key as actID, gigID, actfee, ontime, and duration are not supposed to be unique.
CREATE TABLE act_gig(
    actID INTEGER NOT NULL REFERENCES act(actID),
    gigID INTEGER NOT NULL REFERENCES gig(gigID),
    actfee INTEGER NOT NULL CHECK (actfee >= 0),
    ontime TIMESTAMP NOT NULL,
    duration INTEGER NOT NULL CHECK (duration >= 0)
);

CREATE TABLE venue(
    venueid SERIAL NOT NULL PRIMARY KEY,
    venuename VARCHAR(100) NOT NULL,
    hirecost INTEGER NOT NULL CHECK (hirecost >= 0),
    capacity INTEGER NOT NULL CHECK (capacity >= 0)
);

-- Notice that here is no primary key as gigID, pricetype, and cost are not supposed to be unique.
CREATE TABLE gig_ticket(
    gigID INTEGER NOT NULL REFERENCES gig(gigID),
    pricetype VARCHAR(2) NOT NULL,
    cost INTEGER NOT NULL CHECK (cost >= 0)
);

CREATE TABLE ticket(
    ticketid SERIAL NOT NULL PRIMARY KEY,
    gigID INTEGER NOT NULL,
    pricetype VARCHAR(2) NOT NULL,
    cost INTEGER NOT NULL CHECK (cost >= 0),
    CustomerName VARCHAR(100) NOT NULL,
    CustomerEmail VARCHAR(100) NOT NULL
);


/* [Option 1 Gig Line-Up]: This option is to find all the acts along with their ontime, and offtime given by a gig ID.
   This option first create a VIEW gigTimeTable listing actname along with its ontime, offtime and duration for all gigs and then select some rows with a given gig ID*/
-- Use ontime plus duration to obtain an offtime (offtime = use ontime + duration * interval'1 minute').
CREATE VIEW gigTimeTable AS SELECT actname, gigID, ontime, ontime + duration * interval'1 minute' as offtime, duration FROM act JOIN act_gig USING(actID) order by ontime;
-- To keep ontime's time part, use to_char(ontime, 'HH24:MI:SS').
CREATE view option1 AS SELECT actname, gigID, to_char(ontime,'HH24:MI:SS') as ontime, to_char(offtime,'HH24:MI:SS') as offtime FROM gigTimeTable;


/* [Option 2: Organising a Gig]: This option is to create a new gig.*/
/* PROCEDURE insergGig insert a gig's information into TABLE gig with venue's name, gig's title and gigdate.*/
CREATE OR REPLACE PROCEDURE insertGig(venue_name VARCHAR(100), gig_title VARCHAR(100), on_time TIMESTAMP)
LANGUAGE plpgsql AS $$
DECLARE
    venue_id INTEGER;
BEGIN
--     Retrieve venue's ID from its name.
    SELECT venueid INTO venue_id FROM venue WHERE venuename = venue_name;
--     Insert process.
    INSERT INTO gig VALUES (DEFAULT, venue_id, gig_title, on_time, 'GoingAhead');
END
$$;

/* FUNCTION setupGig call PROCEDURE insertGig to insert a new gig into gig table and meanwhile return its serial ID.*/
CREATE OR REPLACE FUNCTION setupGig(venue_name VARCHAR(100), gig_title VARCHAR(100), on_time TIMESTAMP)
RETURNS INTEGER
LANGUAGE plpgsql AS $$
DECLARE
    gig_id INTEGER;
BEGIN
--     Call PROCEDURE insertGig to do the insertation process.
    CALL insertGig(venue_name, gig_title, on_time);
--     Select and return the newly generated serial ID for the inserted gig.
    SELECT gigID INTO gig_id FROM gig JOIN venue USING(venueid) WHERE gigtitle = gig_title AND gigdate = on_time AND venuename = venue_name;
    RETURN gig_id;
END
$$;

/* PROCEDURE insertGigTicket insert gig's ticket information into TABLE gig_ticket.*/
CREATE OR REPLACE PROCEDURE insertGigTicket(gig_id INTEGER, price_type VARCHAR(2), type_cost INTEGER)
LANGUAGE plpgsql AS $$
DECLARE
BEGIN
    INSERT INTO gig_ticket VALUES (gig_id, price_type, type_cost);
END
$$;

/* PROCEDURE insertActGig insert act's performance information into TABLE act_gig.*/
CREATE OR REPLACE PROCEDURE insertActGig(act_id INTEGER, gig_id INTEGER, act_fee INTEGER, on_time TIMESTAMP, act_duration INTEGER)
LANGUAGE plpgsql AS $$
DECLARE
BEGIN
    INSERT INTO act_gig VALUES (act_id, gig_id, act_fee, on_time, act_duration);
END
$$;

/* Check whether the inserted gig follow the criteria including:
(0)* GIG NOT FOUND: gig is not found with the given ID. However, it does not necessarily mean the data in the database is illegal (could be user's wrong input of gigID).
(1) TIME CONFLICT: act's performance overlaps or act starts before the gig date.
(2) TIME INTERVAL TOO LONG: act's performance gap is larger than 20 minutes or the first act starts 20 minutes later than the gig date.
(3) ACT OVERTIME: an act plays longer than 2 hours.
(4) DATE CROSSED: acts in a given gig plays on different date (which means crossing the midnight).
(5) VENUE OVERLOAD: the ticket sold is greater than the venue capacity.
Return [TRUE] if criteria is not followed*/
CREATE OR REPLACE FUNCTION checkCriteria(gig_id INTEGER)
RETURNS BOOLEAN
LANGUAGE plpgsql AS $$
DECLARE
    table_rank INTEGER;
    act_name VARCHAR(100);
    gig_date timestamp;
    venue_capacity INTEGER;
    current_order INTEGER;
BEGIN
--     (0)* GIG NOT FOUND: gig is not found with the given ID. However, it does not necessarily mean the data in the database is illegal (could be user's wrong input of gigID).
    SELECT gigdate INTO gig_date FROM gig WHERE gigid = gig_id;
    IF NOT FOUND THEN
        RAISE NOTICE 'GIG NOT FOUND';
        RETURN FALSE;
    END IF;
    
    /* Create a table to list actname, gigID, ontime, offtime, duration, previous act's offtime (or gigdate), next act's ontime */
    DROP VIEW IF EXISTS actTimeTable, actgigTimeTable CASCADE;
--     LAG(offtime, 1) is to retrieve the previous performance's offtime, while LEAD(ontime, 1) is to retrieve the next performance's ontime.
    EXECUTE 'CREATE VIEW actTimeTable AS select ROW_NUMBER() OVER(ORDER BY ontime) AS rank, actname, gigid, ontime, offtime, duration, lag(offtime, 1) over(order by ontime) as previous, lead(ontime, 1) over(order by ontime) as next from gigTimeTable where gigid = ' || gig_id;
--     COALESCE(previous, gig_date) is to set the previous performance's offtime as the gig date.
    EXECUTE 'CREATE VIEW actgigTimeTable AS SELECT rank, actname, gigid, ontime, offtime, duration, COALESCE(previous, ' || '''' || gig_date || '''' || ') as previous, next from actTimeTable';
    
    /* Check Process */
    FOR table_rank IN SELECT rank FROM actgigTimeTable LOOP
        SELECT actname INTO act_name FROM actgigTimeTable WHERE rank = table_rank;
--         (1) TIME CONFLICT: act's performance overlaps or act starts before the gig date.
        IF  ontime < previous FROM actgigTimeTable WHERE rank = table_rank THEN RAISE NOTICE 'GIG %: TIME CONFLICT', $1; RETURN TRUE; END IF; 
        
--         (2) TIME INTERVAL TOO LONG: act's performance gap is larger than 20 minutes or the first act starts 20 minutes later than the gig date.
        IF  ontime - previous > INTERVAL '20 minutes' FROM actgigTimeTable WHERE rank = table_rank THEN RAISE NOTICE 'GIG %: TIME INTERVAL TOO LARGE', $1; RETURN TRUE; END IF;
        
--         (3) ACT OVERTIME: an act plays longer than 2 hours.
        IF  duration > 120 FROM actgigTimeTable WHERE rank = table_rank THEN RAISE NOTICE 'GIG %: ACT OVERTIME', $1; RETURN TRUE; END IF;
        
--         (4) DATE CROSSED: acts in a given gig plays on different date (which means crossing the midnight).
        IF  to_char(ontime, 'yyyy-mm-dd') <> to_char(offtime, 'yyyy-mm-dd') FROM actgigTimeTable WHERE rank = table_rank THEN RAISE NOTICE 'GIG %: DATE CROSSED', $1; RETURN TRUE; END IF;
        
--         (5) VENUE OVERLOAD: the ticket sold is greater than the venue capacity.
        SELECT capacity INTO venue_capacity FROM gig JOIN venue USING(venueid) WHERE gigid = gig_id;
        SELECT COUNT(*) INTO current_order FROM ticket WHERE gigid = gig_id;
        IF venue_capacity < current_order THEN RAISE NOTICE 'GIG %: VENUE OVERLOAD', $1; RETURN TRUE; END IF;
    END LOOP;
--     Return FALSE if criteria is not broken.
    RETURN FALSE;
END
$$;

CREATE OR REPLACE PROCEDURE checkAllCriteria()
LANGUAGE plpgsql AS $$
DECLARE
    gig_id INTEGER;
    gig_status BOOLEAN;
BEGIN
    FOR gig_id IN SELECT gigID FROM gig WHERE gigstatus <> 'Cancelled' LOOP
        SELECT * INTO gig_status FROM (SELECT * FROM checkCriteria(gig_id)) temp;
    END LOOP;
END
$$;

/* [Option 3 Booking a Ticket]: This procedure is to insert a customer's ticket information into TABLE ticket.*/
CREATE OR REPLACE PROCEDURE insertTicket(gig_id INTEGER, price_type VARCHAR(2), customer_name VARCHAR(100), customer_email VARCHAR(100))
LANGUAGE plpgsql AS $$
DECLARE
    ticket_cost INTEGER;
    gig_title VARCHAR(100);
    venue_capacity INTEGER;
    current_order INTEGER;
BEGIN
    SELECT cost INTO ticket_cost FROM gig_ticket WHERE gigid = gig_id AND pricetype = price_type;
--     The reason why the cost of a gig is not found can be: (1) GIG NOT FOUND; (2) PRICETYPE NOT FOUND. 
    IF NOT found THEN
        SELECT gigtitle INTO gig_title FROM gig WHERE gigid = gig_id;
        IF NOT found THEN
            RAISE NOTICE 'GIG NOT FOUND';
        ELSE
            RAISE NOTICE 'PRICETYPE NOT FOUND';
        END IF;
    ELSE
--     Check capacity of the venue and current amount of ticket sold.
        SELECT capacity INTO venue_capacity FROM gig JOIN venue USING(venueid) WHERE gigid = gig_id;
        SELECT COUNT(*) INTO current_order FROM ticket WHERE gigid = gig_id;
--         If current amount of ticket sold is equal or greater than the venue capacity, then there is no seat.
        IF current_order >= venue_capacity THEN
            RAISE NOTICE 'NO AVAILABLE SEAT';
        ELSE
--         If no error is encounter then insert the customer's ticket information into TABLE ticket.
            INSERT INTO ticket VALUES (DEFAULT, gig_id, price_type, ticket_cost, customer_name, customer_email);
        END IF;
    END IF;
END
$$;


/* [Option 4: Cancelling an Act]: This option is to cancel a certain act from a specified gig.*/
/* FUNCTION removeActfromGig removes an act from a gig and then check whether the current gig follows the criteria. If not then cancel the entire gig.*/
CREATE OR REPLACE FUNCTION removeActfromGig(gig_id INTEGER, act_name VARCHAR(100))
RETURNS BOOLEAN
LANGUAGE plpgsql AS $$
DECLARE
    act_id INTEGER;
    act_name_headline VARCHAR(100);
    max_interval INTERVAL;
    gig_check BOOLEAN;
    match_check TIMESTAMP;
BEGIN
--     If a given actname is not found, then raise a "ACT NOT FOUND" notice. 
--     It is likely caused by an input of actname which does not exist.
    SELECT actid INTO act_id FROM act WHERE actname = act_name;
    IF NOT FOUND THEN
        RAISE NOTICE 'ACT NOT FOUND';
        RETURN FALSE;
    END IF;
--     If a headline is not found, it means the gig does not exist because ever gig must have a headline, and therefore raise a "GIG NOT FOUND" notice.
--     It is likely caused by an input of gig id which does not exist.
    SELECT actname INTO act_name_headline FROM option1 WHERE gigid = gig_id ORDER BY ontime DESC limit 1;
    IF NOT FOUND THEN
        RAISE NOTICE 'GIG NOT FOUND';
        RETURN FALSE;
    END IF;

--      If an ontime is not found , it means the act does not exist in this gig.
--      The act and the gig may exist but they are not in the matching.
    SELECT ontime INTO match_check FROM act_gig WHERE actid = act_id AND gigid = $1 LIMIT 1;
    IF NOT FOUND THEN
        RAISE NOTICE 'ACT NOT EXIST IN THIS GIG';
        RETURN FLASE;
    END IF;
    
    /* Delete process */
    DELETE FROM act_gig WHERE gigid = gig_id AND actid = act_id;
    
    /* Check Headline Act: whether the act removed is the headline of the gig.*/
    IF act_name = act_name_headline THEN
        UPDATE gig SET gigstatus = 'Cancelled' WHERE gigid = gig_id;
--         Cancel the entire gig if the act removed is the headline. 
        RAISE NOTICE 'CANCEL GIG DUE TO HEADLINE ACT';
        RETURN TRUE;
    /* Check Criteria: check whether the removal of the act will lead to a breach of criteria (including interval gap) */
    ELSE
        EXECUTE 'SELECT * INTO gig_check FROM checkCriteria(' || $1 || ')';
        IF gig_check IS TRUE THEN
--         Cancel the entire gig if the removal lead to a breach of criteria. Further notice is raised during the check process.
            UPDATE gig SET gigstatus = 'Cancelled' WHERE gigid = gig_id;
            RAISE NOTICE 'CANCEL GIG DUE TO INTERVAL / TIME CONFLICT';
            RETURN TRUE;
        END IF;
    END IF;
    RETURN FALSE;
END
$$;

/* PROCEDURE setAffectedTicket is to set the cost to 0 for all tickets which is cancelled (as the gig is cancelled) and create a view include the affected customer's email.*/
CREATE OR REPLACE PROCEDURE setAffectedTicket(gig_id INTEGER)
LANGUAGE plpgsql AS $$
DECLARE
BEGIN
    DROP VIEW IF EXISTS VIEW_setAffectedTicket;
--     VIEW VIEW_setAffectedTicket: inlucdes a list of emails of whose tickets are cancelled.
    EXECUTE 'CREATE VIEW VIEW_setAffectedTicket AS SELECT CustomerEmail FROM ticket WHERE gigid = ' || $1;
--     Set the cost to 0 for tickets of a given gigID.
    UPDATE ticket SET cost = 0 WHERE gigid = gig_id;
END
$$;


/* [Option 5: Tickets Needed to Sell]: This option show how many "A"(adult) tickets need selling to reimburse the expense. */
-- VIEW totalActFee: includes the total actfees for gigs (for each by adding its actfees using SUM(actfee));
CREATE VIEW totalActFee AS SELECT gigid, SUM(actfee) AS total_act_fee FROM act_gig GROUP BY gigid ORDER BY gigid;
-- VIEW venueFee: includes the hirecost of required venues.
CREATE VIEW venueFee AS SELECT gigid, hirecost FROM gig JOIN venue USING(venueid) ORDER BY gigid;
-- VIEW totalTicketIncome includes the total ticket income for different gigs. If no ticket has been sold, it will be 0.
CREATE VIEW totalTicketIncome AS SELECT gigid, COALESCE(SUM(cost), 0) AS total_ticket_income FROM gig LEFT OUTER JOIN ticket USING(gigid) GROUP BY gigid ORDER BY gigid;
-- VIEW balance gathers all information from VIEW totalActFee, venueFee, and totalTicketIncome along with cost to reimburse.
CREATE VIEW balance AS SELECT gigid, total_act_fee, hirecost, total_act_fee + hirecost AS total_cost, total_ticket_income, total_act_fee + hirecost - total_ticket_income AS cost_to_reimburse FROM totalactfee JOIN venuefee USING(gigid) JOIN totalticketincome USING(gigid) ORDER BY gigid;
-- VIEW ticketToSell includes the amount of adult tickets need selling to pay the cost_to_reimburse from VIEW balance.
CREATE VIEW ticketToSell AS SELECT gigid, cost_to_reimburse, cost AS adult_ticket_price, CEILING(CAST(cost_to_reimburse AS FLOAT)/COST) AS adult_ticket_to_sell FROM balance JOIN gig_ticket USING(gigid) WHERE pricetype = 'A' ORDER BY gigid;
-- Notice that if a gig has no pricetype 'A', it will still show on the VIEW balance, but not on the VIEW ticketToSell.


/* [Option 6: How Many Tickets Sold]: This option shows the amount of tickets an act sold as a headline. */
-- VIEW headlinetime: includes gigID and its headline's ontime (along with gig's year).
CREATE VIEW headlinetime as select gigid, max(ontime) as ontime, date_part('year', max(ontime)) as year from act_gig group by gigid order by gigid;
-- VIEW gigHeadline: includes gigID and its headline's actname (along with gig's year).
CREATE VIEW gigHeadline as select act_gig.gigid, actname, year from act_gig join headlinetime using(ontime) join act using(actid);
-- VIEW ticketSold: includes gigID and the amount of ticket sold if the gig is going ahead (not "Cancelled").
CREATE VIEW ticketSold as select gigid, count(*) as ticket_sold from ticket join gig using(gigid) where gigstatus <> 'Cancelled' group by gigid order by gigid;
-- VIEW actYearTicket: by combining gigHeadline and ticketSold, it includes the name of acts which used to be headline, year of being headline, and amount of tickets of gig as this act being a headline for different years.
CREATE VIEW actYearTicket as select actname, year::text, sum(ticket_sold) as year_ticket_sold from gigheadline join ticketsold using(gigid) group by (actname, year) order by actname, year;
-- VIEW actTotalTicket: by adding up the amount of tickets of different years, it includes the name of acts which used to be headline along with total amount of tickets of gig as this act being a headline.  
CREATE VIEW actTotalTicket as select actname, 'Total' as year, sum(year_ticket_sold), ROW_NUMBER() OVER(ORDER BY sum(year_ticket_sold)) AS rank from actyearticket group by actname order by sum;
-- VIEW rankedActYearTicket: it is a ranked view of actYearTicket in an ascending order of total amount of tickets of gig as an act being a headline.
CREATE VIEW rankedActYearTicket as select actname, actyearticket.year, year_ticket_sold, rank from actyearticket left outer join acttotalticket using(actname) order by rank, year;
-- VIEW actUnionTicket: it is a union of VIEW actTotalTicket and rankedActYearTicket, which combines the amount of ticket of "year" and "total" and meanwhile is in a required order (with act name staying together in a block, the block inside is in an order of year with "Total" at last). To achieve the actname block, the VIEW actTotalTicket ranks the actname in an order of ascending amount of total tickets sold. This ranking is inheritted by rankedActYearTicket. Therefore, For actUnionTicket, it must summon all the same actname together as a block. Within the block, it is going to rank by the "Year" column in an order of smaller year - larger year - "Total" (comparable as they are all text).
CREATE VIEW actUnionTicket AS select actname, year, year_ticket_sold from ((select * from rankedactyearticket) union (select * from acttotalticket) order by rank, year) tempUnion;


/* [Option 7 Regular Customers]: This option shows a list of regular customers of acts. Regular customer (RC) definition: a customer of the act who buys the ticket of this act being a headline for at least once every "year" (as the "year" means that an act used to be a headline in this year).*/
-- VIEW gigTicket: combine the ticket and gigheadline (which is a list of headline of the gigs) by matching the same gigID. Therefore it stores the customer information along with the gigID, name of the headline, and the year of performance. Notice that a gig which is cancelled should be shown here.    
CREATE VIEW gigTicket AS select gigid, actname, year, ticketid, customername, customeremail from gigHeadline left outer join ticket using(gigid) join gig using(gigid) where gigstatus <> 'Cancelled';
-- VIEW actHeadlineList: includes name of the headlines and its ranking of alphabetical order (as required).
CREATE VIEW actHeadlineList AS SELECT ROW_NUMBER() OVER(ORDER BY actname) AS RANK, actname FROM (select DISTINCT actname from gigHeadline) temp;
-- VIEW actHeadlineYear: includes name of the headlines and the year of performance being a headline.
CREATE VIEW actHeadlineYear AS select DISTINCT actname, year from gigTicket order by actname, year;
-- VIEW annualHeadline: includes name of the headlines and the amount of the years that an act being a headline for at least once in a year.
CREATE VIEW annualHeadline AS select actname, count(*) from actheadlineyear group by actname;

/*PROCEDURE getRC is to get all regular customers given an actname and the rank of alphabetical order (as required).*/
CREATE OR REPLACE PROCEDURE getRC(act_name VARCHAR(100), act_rank INTEGER)
LANGUAGE plpgsql AS $$
DECLARE
    headline_year VARCHAR(5);
    first_headline_year VARCHAR(5);
    previous_headline_year VARCHAR(5);
    last_headline_year VARCHAR(5);
    customer_name VARCHAR(100);
    empty_customer_test VARCHAR(100);
    null_test INTEGER;
    RC_ticket_amount INTEGER;
    
    
BEGIN
        SELECT year INTO first_headline_year FROM actHeadlineYear WHERE actname = act_name limit 1;
        SELECT year INTO last_headline_year FROM actHeadlineYear WHERE actname = act_name ORDER BY year DESC limit 1;
        
        EXECUTE 'DROP VIEW IF EXISTS union' || first_headline_year || '_' || act_rank ||' CASCADE';
        EXECUTE 'CREATE VIEW union' || first_headline_year || '_' || act_rank || ' AS SELECT DISTINCT actname, customername, customeremail, year::TEXT FROM gigticket WHERE actname = ' || '''' || act_name || '''' || ' AND year::TEXT = ' || '''' || first_headline_year || '''';
        
--         For a given actname, this part is to collect all customer's information who buy a ticket of a gig as the actname is the headline for all years.
        FOR headline_year IN SELECT year::TEXT FROM actHeadlineYear WHERE actname = act_name LOOP
            EXECUTE 'DROP VIEW IF EXISTS regular' || headline_year || '_' || act_rank || ' CASCADE';
--             The view is named by regular first, and then the year of which the given act used to be the headline, and finally the actname's alphabetical order, e.g. regular2017_1.
            EXECUTE 'CREATE VIEW regular' || headline_year || '_' || act_rank || ' AS SELECT DISTINCT gigid, actname, customername, customeremail, year::TEXT FROM gigticket WHERE actname = ' || '''' || act_name || '''' || ' AND year::TEXT = ' || '''' || headline_year || '''';
        END LOOP;
        
--         VIEW lagYear is to derive the last year of which an act used to be the headline, which is important. For example, an act used to be the headline in 2018. It does not        necessarily mean that the last time it be a headline is in 2017 (could be 2016). Therefore, the view provides a correct "last year" of the act being the headline.
        DROP VIEW IF EXISTS lagYear CASCADE;
        EXECUTE 'CREATE VIEW lagYear AS select * from (select year, lag(year, 1) over(order by year) from actheadlineyear where actname = ' || '''' ||act_name || '''' || ') temp';
        
--         For a given actname, this part is to union all the customers who used to buy tickets of a gig that the given act is the headline before a certain year. If this year is the last year that an act played as headline, then this part will include all potential regular customers (who buy at least one ticket of a gig that the given act is the headline).
        FOR headline_year IN SELECT year::TEXT FROM actHeadlineYear WHERE actname = act_name LOOP
            IF headline_year = first_headline_year THEN
                CONTINUE;
            END IF;
            EXECUTE 'DROP VIEW IF EXISTS union' || headline_year || '_' || act_rank || ' CASCADE';
            SELECT lag::text INTO previous_headline_year FROM lagYear WHERE year::text = headline_year;
--             The view is named by union first, and then the year of which the given act used to be the headline, and finally the actname's alphabetical order, e.g. union2017_1. Such a view includes all customers who used to buy a ticket of a gig which an act is the headline. For example, an act whose alphabetical order is 1 used to be headline on 2016, 2017, and 2019. In this case, we have regular2016_1, regular2017_1, and regular2019_1. union2016_1 includes regular2016_1, union2017_1 includes union2016_1 and regular2017_1, and union2019_1 includes union2017_1 and regular2019_1. Therefore, all customers are covered.
            EXECUTE 'CREATE VIEW union' || headline_year || '_' || act_rank || ' AS (SELECT DISTINCT actname, customername, customeremail, year::TEXT FROM union' || previous_headline_year || '_' || act_rank || ' UNION SELECT DISTINCT actname, customername, customeremail, year::TEXT FROM regular' || headline_year || '_' || act_rank || ')';
        END LOOP;
        
--         VIEW groupRC_act_rank is to count the number of each customers buying tickets of headline gigs for different years. If a customer is a RC, then the amount should be equal to the amount of year which an act used to be a headline in a year.
        EXECUTE 'DROP VIEW IF EXISTS groupRC_' || act_rank || ', selectedRC_' || act_rank || ', sortedRC_' || act_rank || ' CASCADE';
        EXECUTE 'CREATE VIEW groupRC_' || act_rank || ' AS select actname, COALESCE(customeremail, ' || '''[None]''' || '), COALESCE(customername, ' || '''[None]''' || ') AS customername, count(*) from union' || last_headline_year || '_' || act_rank || ' group by (actname, customeremail, customername)';
        
--         VIEW selectedRC_act_rank picks the RC from the VIEW groupRC_act_rank, by comparing the times that a customer buys for a gig of act being headline per year.
        EXECUTE 'CREATE VIEW selectedRC_' || act_rank || ' AS select actname, customername from groupRC_' || act_rank || ' join annualheadline using(actname) where groupRC' || '_' ||act_rank || '.count = annualheadline.count';
        
--         VIEW selectedRC_null is to test whether a selectedRC_act_rank owns any row or not. If not, it means there are no RC for this act, and therefore should return [None].
        DROP VIEW IF EXISTS selectedRC_null;
        EXECUTE 'CREATE VIEW selectedRC_null AS SELECT * FROM selectedRC_' || act_rank;
--         Count the row number of selectedRC_null (selectedRC_act_rank).
        SELECT COUNT(*) INTO null_test FROM selectedRC_null;
        IF null_test = 0 THEN
            EXECUTE 'DROP VIEW selectedRC_' || act_rank || ' CASCADE';
            EXECUTE 'CREATE VIEW selectedRC_' || act_rank || ' AS select ' || '''' || act_name || '''' || ' as actname, ' || '''[None]''' || ' as customername';
        END IF;
        
--         VIEW comparedRC_act_rank contains customers and then rank them by comparing the amount of tickets they buy for a gig of an act being the headline.
        EXECUTE 'CREATE VIEW comparedRC_' || act_rank || ' AS select ' || act_rank || ' AS rank, selectedrc_' || act_rank || '.actname, selectedrc_' || act_rank || '.customername, COALESCE(gigticket.customername, '|| '''[None]''' || '), COUNT(*) from selectedrc_' || act_rank || ' JOIN gigticket using(actname) group by (selectedrc_' || act_rank || '.actname, selectedrc_' || act_rank || '.customername, gigticket.customername)';
        
--         VIEW sortedRC_act_rank includes customers who is actually RC whose times that buying for a gig of act being headline per year equals to the total one.
        EXECUTE 'CREATE VIEW sortedRC_' || act_rank || ' AS select rank, actname, customername, count FROM comparedRC_' || act_rank || ' WHERE customername = coalesce';

END
$$;

/* PROCEDURE getAllRC is to find all the RC for each act by calling PROCEDURE getRC with actname and alphabetical act rank as parameters.*/
CREATE OR REPLACE PROCEDURE getAllRC()
LANGUAGE plpgsql AS $$
DECLARE
    act_rank INTEGER;
    act_name VARCHAR(100);
    last_rank INTEGER;
BEGIN
-- This part calls PROCEDURE getRC for each act to form a view of RC of the act.
--     This rank is actname's alphabetical ranking.
    FOR act_rank IN SELECT rank FROM actHeadlineList LOOP
        SELECT actname INTO act_name FROM actHeadlineList WHERE rank = act_rank;
        CALL getRC(act_name, act_rank);
    END LOOP;
    
    -- This part collects all RC for different acts before its alphabetical order (inclusively).
    FOR act_rank IN SELECT rank FROM actHeadlineList LOOP
--         The view is named by unionRC first, and then the alphabetical order.
--         UnionRC_1 should be detected as 1 has no prior order.
        IF act_rank = 1 THEN
            DROP VIEW IF EXISTS unionRC_1 CASCADE;
            CREATE VIEW unionRC_1 AS SELECT * FROM sortedRC_1;
            CONTINUE;
        END IF;
--         UnionRC_act_rank is to combine unionRC of the last order and sortedRC of current order. Therefore, all acts are covered.
        EXECUTE 'DROP VIEW IF EXISTS unionRC_' || act_rank || ' CASCADE';
        EXECUTE 'CREATE VIEW unionRC_' || act_rank || ' AS (SELECT * FROM unionRC_' || (act_rank - 1)::text || ' UNION SELECT * FROM sortedRC_' || act_rank || ' ORDER BY rank, count DESC, customername)';
    END LOOP;
    
-- This part collects all RC for different acts into VIEW preparedRC, which is proper to be a feedback of option 7.
    /* VIEW preparedRC includes:
    (1) actname: name of the act which used to be a headline.
    (2) customername: name of RC.

    To get (1) actname in VIEW preparedRC, first we form a view which includes the gigID, the name of headline (by obtaining the largest ontime and matching the act), and the year of  performance. Based on this view, we create another view with distinct year, which lists the years of an act used to be a headline (as RC is required to buy a ticket from at least one ticket for each).
    
    To get (2) customername, based on the actname and year we derived from the last view, we check whether a customer buy at least one ticket for each year's gig which a certain act is the headline. If so, we add this name to the actname column.*/
    
--     As UnionRC of the largest rank covers all acts, we select the max rank and feedback unionRC of this rank order by rank(between acts), count(between RC), and customername(if count is the same).
    SELECT MAX(rank) INTO last_rank FROM actHeadlineList;
    EXECUTE 'CREATE VIEW preparedRC AS SELECT actname, customername FROM unionRC_' || last_rank || ' ORDER BY rank, count DESC, customername'; 
END
$$;

/* [Option 8: Economically Feasible Gigs]: This option is to provide a list of economically feasible gigs.
    Economically Feasible Gig Definition: a gig can reimburse the venue hirecost and the act standardfee by selling ticket of average price within the venue capacity limit.
    Proportion of tickets Definition: amount of tickets required to reimburse total expense (hirecost and standardfee) / venue capacity.
*/
CREATE OR REPLACE PROCEDURE searchFeasibleGig()
LANGUAGE plpgsql AS $$
DECLARE
    ticketIncome FLOAT;
    ticketAmount INTEGER;
    ticketAveragePrice FLOAT;
BEGIN
--     FLOAT ticketIncome: calculates the sum of all tickets of gigs which are not cancelled.
    select sum(cost)::FLOAT INTO ticketIncome from ticket join gig using(gigid) where gigstatus <> 'Cancelled';
--     INTEGER ticketAmount: calculates the amount of all tickets of gigs which are not cancelled.
    select count(cost) INTO ticketAmount from ticket join gig using(gigid) where gigstatus <> 'Cancelled';
--     FLOAT ticketAveragePrice: average price of all tickets of gigs which are not cancelled.
    select ticketIncome / ticketAmount INTO ticketAveragePrice;
    
    DROP VIEW IF EXISTS actVenue, feasiblegigList, feasiblegigTicket, sortedFeasibleGig CASCADE;
--     VIEW actVenue: lists all matching of venue and act along with their cost by cross join of TABLE venue and act.
    CREATE VIEW actVenue AS select venuename, actname, hirecost, standardfee, hirecost + standardfee as total_cost, capacity from venue cross join act;
--     VIEW feasibleGigList: includes name of venue and name of act which are an economically feasible matching, cost (venue's hirecost and act's standardfee), and venue's capacity.
    EXECUTE 'CREATE VIEW feasibleGigList AS SELECT venuename, actname, total_cost, capacity FROM actVenue WHERE capacity * ' || ticketAveragePrice || ' >= total_cost';
    
--     VIEW feasibleGigTicket: based on VIEW feasibleGigList, it includes an extra column ticket_required.
    /* To get ticket_required:
    first we create a VIEW actVenue by cross joining act and venue, so we can derive the total cost (venue's hirecost and act's standardfee). Then we calculate the average price of ticket (of gigs which are not cancelled). Based on the average price, we can calculate the maximum income (average price * capacity). We use this income to minus the total cost, which is the pure interest. As the option aims to "get even", we need to get the amount of tickets required to reimburse the total cost. Therefore, we divide the pure interest by the average price of tickets, which means the maximum amount of tickets that we do not need to "get even". After that, we use capacity to minus this maximum amount of tickets we don't need and then get the least amount of tickets that we need to "get even". Notice that a ceiling is required here as decimal digits does not work for the amount of ticket.
    */
    EXECUTE 'CREATE VIEW feasibleGigTicket AS select *, CEILING(capacity - (capacity * ' || ticketAveragePrice || ' - total_cost) / ' || ticketAveragePrice || ') as ticket_required from feasiblegiglist';
--     VIEW sortedFeasibleGig: based on VIEW feasiblegigTicket, it ranks the rows in an order of venuename, proportion of venue usage (descending) to reimburse the cost, and actname.
    CREATE VIEW sortedFeasibleGig AS SELECT venuename, actname, ticket_required FROM feasiblegigTicket ORDER BY venuename, ticket_required::FLOAT / capacity DESC, actname;

END
$$;
