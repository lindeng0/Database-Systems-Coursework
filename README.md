# Design Choices

### (1) Table Structure

##### Need of Primary Key of Joint Table: 

TABLE act_gig and gig_ticket as joint table, have no primary key as their columns are either referenced or improper to be unique. In this case, though retrieving data in these tables are not necessarily rough (sometimes it even builds a quick connection between separate tables), it is still recommended to add a primary key for better identification and independence. Otherwise, these joint table are vulnerable if the main table changes (e.g., data of act_gig becomes "stranded" when a gig is cancelled). A serial ID or a "String" ID combined with vital information (e.g., S10000BH: "S" for schedule, "10000" for gig's ID and "BH" for venue name ”Big Hall").

##### More Column with Necessary Information: 

Some table has all necessary information but meanwhile they are scattered. For example, offtime can be derived by adding ontime and duration together, however, it would be more convenient to have an extra column "offtime" as it is a more intuitionistic information, not to mention the "offtime" is actually frequently used. Besides, some table actually do not have all necessary information which is vital in real life. To illustrate, many gigs require that a ticket is fixed to a specific seat (instead of seating wherever a customer want), so for the TABLE ticket, it would be better to have a COLUMN seatNumber.

### (2) Option Design

##### A More "Accurate" Output:

For most options, the aims are clear and straightforward, but the output may need a more careful design. For example, option 7 requires a list of regular customers of a certain act. The original expectation may be finding a loyal customer. However, by only matching act name with customer name may be troubled by tautonym. For example, assume there is a gentleman named Tom who buys every tickets of gigs that an act being the headline, while there is another Tom who only buy once. In this case, the output will only include one "Tom", which requires further affirmation.

##### A More "Proper" Order:

For some options, their output may not be required in the most appropriate order. For example, option 8 demands an order by venue name first (which is fine), and then the descending proportion (tickets required to reimburse the cost / venue's capacity). With such an order, the user may pay attention to the above ones whose attendance are more difficult to guarantee.



#  Schema Design

### (1) Constraints

First, for most columns, it is important for them to be not null, e.g., the part of ID, name, time, and money, which also covers most part of the data. For some "less important" information such as genre and members (amount), a "not null" check is also be put for consistency. Second, for parameters which are impossible to be negative (e.g., fee, duration, capacity), a check of positive number is added. Third, two tables (act_gig, gig_ticket) have no primary key due to all its columns are referenced from other tables or they are not distinctly identifiable. This situation should be prevent when designing a table.

### (2) Views

Views are frequently used in my schema, which builds step to solve a complex SQL query. Besides, as PostgreSQL is relation based, creating views provides another way to build real time connection between objects. For example, a cancel of gig from [option 4 Remove act from gig] will not influence the correctness of a VIEW gigHeadline created before which lists the name of headline for each gigs which is "Going Ahead".

### (3) Functions and Procedures

In my schema, modifying the database is mostly done by procedures (e.g., inserting gigs for option 2). Besides, procedures are used to create views, which provides a "updating" and "immediate" access to data. Functions can do similar job while it also returns some significant value. Notice that no table is created in this coursework while most job are done by procedures, functions, and views they created.



# Option Design

There are 8 options in total:

### (1) Gig Line-Up: 

This option is to find all the acts along with their ontime, and offtime given by a gig ID. As act's information and their ontime is directly provided, the only parameter which we need to calculate is the offtime. It is obtained by ontime (timestamp) + duration (integer) * INTERVAL '1 minute'. Finally, we keep the HH:MM:SS (HH24:MI:SS) part of the time and return a proper view, allowing JDBC to collect information it wants.

### (2) Organising a Gig: 

This option is to insert data related to gig. Notice that a gig relates 3 tables: gig, act_gig, and gig_ticket. Besides, the times to insert data into these tables are different. For TABLE gig, it takes one time to insert sufficient info, as gig itself is distinct. It is similar for gig_ticket, as there are only one "Adult ticket" standard for a gig. However, for act_gig, it takes n times (as n is the amount of act playing in this gig), which decides the structure of Java program of this option. Notice that a check of criteria is implemented:

​      (1) TIME CONFLICT: act's performance overlaps or act starts before the gig date.

​      (2) TIME INTERVAL TOO LONG: act's playing gap is larger than 20 mins or the first act starts 20 mins later than the gig date.

​      (3) ACT OVERTIME: an act plays longer than 2 hours.

​      (4) DATE CROSSED: acts in a given gig plays on different date (which means crossing the midnight).

Once a criteria breach is detected, database will rollback to the initial save point. Detailed notice will also be raised in PostgreSQL. This checking is achieved by PROCEDURE checkCriteria(gigID INTEGER), which forms a view with the following format:

| actname | gigID | ontime | offtime | duration | previous (act's offtime) | next (act's ontime) |
| :-----: | :---: | :----: | :-----: | -------- | :----------------------: | :-----------------: |
|         |       |        |         |          |                          |                     |

Notice that COALESCE(previous, gigdate) is used to change the 'null' value of previous act's offtime into gig's date. Therefore, we can check time conflict and time interval for acts mutually and for act and gig relatively. For "act overtime", we check duration, and for "date cross", we observe the date part of ontime and offtime.

### (3) Booking a Ticket

This option is to insert a new ticket information into TABLE ticket. Inserting data is through PROCEDURE insertTicket, which is not difficult: INSERT INTO ticket VALUES (DEFAULT, gig_id, price_type, ticket_cost, customer_name, customer_email). But it is important to notice the criteria: for a ticket, its criteria differs with gig's, as it checks potential breach on:

​		(1) GIG NOT FOUND: a given gig does not exist in the database.

​		(2) PRICETYPE NOT FOUND: a given gig exists while the price type does not.

​		(3) NO AVAILABLE SEAT: current amount of sold tickets exceeds venue's capacity.

We check these by trying to retrieve the value. If not found then it means it does not exist in the database.

(4) Cancelling an Act

This option is to cancel a certain act from a specified gig. The removing process is achieved by a simple SQL command: DELETE FROM act_gig with a given gigID and actID. However, its influence is tricky to handle, as we need to consider:

​		(1) CANCEL GIG DUE TO HEADLINE ACT

​		(2) CANCEL GIG DUE TO INTERVAL / TIME CONFLICT

A headline can be targeted by comparing the ontime (which should be the last ontime), while the scenario 2 can be handle by FUNCTION checkCriteria(gigID INTEGER) mentioned before.

### (5) Tickets Needed to Sell

This option show how many "A"(adult) tickets need selling to reimburse the expense. To get the expense, we retrieve actfees (act agreed fee) and venue's hirecost. Then we retrieve the price of adult ticket. Finally, we divide the cost by the price and get the required amount. Notice that a CEILING(amount) is used because the amount of ticket must be integer and its benefit must cover (equal to or larger than) the expense.

### (6) How Many Tickets Sold

This option shows the amount of tickets an act sold as a headline. First a list of headline along with gigID is obtained by select largest ontime. Then we match the list with tickets and the problem is solved. However, the difficult part is to rank: according to the requirement, this view first put the same actname together in a "block", and the "block" inside is in an order of year with "Total" at last. In short, it is achieved by ranking the total amount of tickets sold by a headline act first, and then have the rank inherited(copied) by the row which records the yearly amount of tickets sold by the same act. Therefore, the row with a same actname will stay in a "block". Within the block, it is ranked by "Year" column in an order of smaller year - larger year - "Total" (comparable as they are all text).

### (7) Regular Customers

This option shows a list of regular customers of acts. Regular customer (RC) definition: a customer of the act who buys the ticket of this act being a headline for at least once every "year" (as the "year" means that an act used to be a headline in this year). Basically, we derive the list by comparing the times of year that a customer buys tickets. For example, an act play as headline twice in 2016, once in 2017, and once in 2019. It means that the distinct buying times of years ought to be 3 in order to cover all the calendar years. Finally we rank the regular customers list based on the total amount of them buying tickets of gig of a certain act being headline.

### (8) Economically Feasible Gigs

This option is to provide a list of economically feasible gigs. There are two definitions we need to know:

​		Economically Feasible Gig: a gig can reimburse the venue hirecost and the act standardfee by selling ticket of average price within the venue capacity limit.

​		Proportion of tickets Definition: amount of tickets required to reimburse hirecost and standardfee / venue capacity.

To get the least ticket required, first we create a VIEW actVenue by cross joining act and venue, so we can derive the total cost (venue's hirecost and act's standardfee). Then we calculate the average price of ticket (of gigs which are  not cancelled). Based on the average price, we can calculate the maximum income (average price * capacity). We use this income to minus the total cost, which is the pure interest. As the option aims to "get even", we need to get the amount of tickets required to reimburse the total cost. Therefore, we divide the pure interest by the average price of tickets, which means the maximum amount of tickets that we do not need to "get even". After that, we use capacity to minus this maximum amount of tickets we don't need and then get the least amount of tickets that we need to "get even". Notice that a ceiling is required here as decimal digits does not work for the amount of ticket.