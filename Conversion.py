import pyodbc
from datetime import date
from datetime import datetime

today = date.today()

class Contractor:
	Contractors = None
	BPhone = None
	FPhone = None
	OPhone = None
	BAddress1 = None
	BAddress2 = None
	MAddress1 = None
	MAddress2 = None
	License = None
	First = None
	Middle = None
	Last = None
	iscompany = None   
	isperson = None
	EDate = date.today()
	PTYPE1 = None
	PTYPE2 = None
	PTYPE3 = None
	Role = None




sourceCon = (
    r'DRIVER={ODBC Driver 17 for SQL Server};'  
    r'SERVER=JamesB;'
    r'DATABASE=source_db;'
    r'Trusted_Connection=yes;'
)
connection = pyodbc.connect(sourceCon)

destinationCon = (
    r'DRIVER={ODBC Driver 17 for SQL Server};'  
    r'SERVER=JamesB;'
    r'DATABASE=destination_db;'
    r'Trusted_Connection=yes;'
)
connection2 = pyodbc.connect(destinationCon)

certificationConnection = (
    r'DRIVER={ODBC Driver 17 for SQL Server};'  
    r'SERVER=JamesB;'
    r'DATABASE=source_db;'
    r'Trusted_Connection=yes;'
)
connection3 = pyodbc.connect(certificationConnection)



query = "SELECT * FROM dbo.Contractor"
query2  =  """
	INSERT INTO dbo.CONTACT (
	    CONTACT_ID, IS_COMPANY, IS_PERSON, COMPANY_NAME, 
	    FIRST_NAME, MIDDLE_NAME, LAST_NAME, 
	    PHONE_TYPE_1, PHONE_NUMBER_1, PHONE_TYPE_2, PHONE_NUMBER_2, 
	    PHONE_TYPE_3, PHONE_NUMBER_3, ENTERED_DATE
	) 
	VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	"""
source_cursor = connection.cursor()
destination_cursor = connection2.cursor()
source_cursor.execute(query)

records = source_cursor.fetchall()
for r in records:
	Contractors = None
	BPhone = r.BusinessPhoneNumber
	FPhone = r.FaxNumber
	OPhone = r.OtherPhoneNumber
	BAddress1 = r.BusinessAddressLine1
	BAddress2 = r.BusinessAddressLine2
	MAddress1 = r.MailingAddressLine1
	MAddress2 = r.MailingAddressLine2
	License = None
	First = r.FirstName
	Middle = r.MiddleInitial
	Last = r.LastName
	iscompany = None   
	isperson = None
	EDate = date.today()
	PTYPE1 = None
	PTYPE2 = None
	PTYPE3 = None


	if r.CompanyName == None or len(r.CompanyName) == 0:
		if(Middle == None):
			isperson = True
			iscompany =  False
		
		else:
			isperson = True
			iscompany = False
		
	else :
		Contractors = r.CompanyName
		iscompany = True
		isperson = False

	PTYPE1 = "Business"
	PTYPE2 = "Fax"
	PTYPE3 = "Other"


	CONTACT_ID = r.ContractorID
	destination_cursor = connection2.cursor()
	destination_cursor.execute("SELECT MAX(CONTACT_ID) + 1 FROM dbo.CONTACT")
	
	insert = (CONTACT_ID, iscompany, isperson, Contractors, First, Middle, Last, PTYPE1,
	 BPhone, PTYPE2, FPhone, PTYPE3, OPhone, EDate) 

	destination_cursor.execute(query2, insert)
	connection2.commit()




def migrate_addresses():
    # SQL query to select business addresses from the source database
    business_address_query = """
    SELECT 
        ContractorID,
        BusinessAddressLine1,
        BusinessAddressLine2,
        BusinessCity,
        BusinessStateCode,
        BusinessZipCode
    FROM source_db.dbo.Contractor
    """

    # SQL query to select mailing addresses from the source database
    mailing_address_query = """
    SELECT 
        ContractorID,
        MailingAddressLine1,
        MailingAddressLine2,
        MailingCity,
        MailingStateCode,
        MailingZipCode
    FROM source_db.dbo.Contractor
    """

    source_cursor.execute(business_address_query)
    business_addresses = source_cursor.fetchall()

    # Fetch mailing addresses
    source_cursor.execute(mailing_address_query)
    mailing_addresses = source_cursor.fetchall()

    #contact_id_query = "SELECT ContactID FROM destination_db.dbo.CONTACT WHERE ContractorID = CONTACT_ID"


    insert_address_query = """
    INSERT INTO destination_db.dbo.ADDRESS (ADDRESS_ID, ADDRESS_LINE_1, ADDRESS_LINE_2, 
    CITY, STATE_CODE, ZIP, ADDRESS_TYPE
    )
    VALUES (?, ?, ?, ?, ?, ?, ?)
    """

    # Function to check if an address already exists
    def address_exists(address_id):
        check_query = "SELECT 1 FROM destination_db.dbo.ADDRESS WHERE ADDRESS_ID = ?"
        destination_cursor.execute(check_query, (address_id,))
        return destination_cursor.fetchone() is not None

    # Migrate business addresses
    for row in business_addresses:
        ADDRESS_ID = row.ContractorID
        if not address_exists(ADDRESS_ID):
            destination_cursor.execute(insert_address_query, (ADDRESS_ID, row.BusinessAddressLine1, row.BusinessAddressLine2, 
                                                              row.BusinessCity, row.BusinessStateCode, row.BusinessZipCode, 'Business'))

    # Migrate mailing addresses
    for row in mailing_addresses:
        ADDRESS_ID = row.ContractorID
        if not address_exists(ADDRESS_ID):
            destination_cursor.execute(insert_address_query, (ADDRESS_ID, row.MailingAddressLine1, row.MailingAddressLine2, 
                                                              row.MailingCity, row.MailingStateCode, row.MailingZipCode, 'Mailing'))

    connection2.commit()


   
def migrate_licenses():
    # SQL query to select licenses from the source database
    license_query = """
    SELECT 
        ContractorLicenseID,
        ContractorID,
        LicenseNumber,
        LicenseType,
        CreateDate,
        IssueDate,
        ExpirationDate,
        LicenseStatus
    FROM source_db.dbo.ContractorLicense
    """

    licenses = connection.execute(license_query).fetchall()

    insert_certification_query = """
    INSERT INTO destination_db.dbo.CERTIFICATION (CERTIFICATION_ID, CONTACT_ID, CERTIFICATION_NUMBER, CERTIFICATION_TYPE,
     CERTIFICATION_STATUS, ENTERED_DATE, ISSUED_DATE, EXPIRATION_DATE)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """

    # Function to convert and validate date
    def convert_date(date_str):
        if date_str:
            try:	
                return datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')  # Adjust format as needed
            except ValueError:
                return None
        return None

    # Function to check if CERTIFICATION_ID exists
    def certification_id_exists(certification_id):
        check_query = "SELECT 1 FROM destination_db.dbo.CERTIFICATION WHERE CERTIFICATION_ID = ?"
        destination_cursor.execute(check_query, (certification_id,))
        return destination_cursor.fetchone() is not None

    # Migrate licenses to certifications
    for row in licenses:
        CERTIFICATION_ID = row.ContractorLicenseID
        CONTACT_ID = row.ContractorID  # Assuming CONTACT_ID is the same as ContractorID, adjust as needed
        create_date = convert_date(str(row.CreateDate))
        issue_date = convert_date(str(row.IssueDate))
        expiration_date = convert_date(str(row.ExpirationDate))

        # Ensure unique CERTIFICATION_ID
        if certification_id_exists(CERTIFICATION_ID):
            print(f"CERTIFICATION_ID {CERTIFICATION_ID} already exists, generating a new unique ID.")
            destination_cursor.execute("SELECT MAX(CERTIFICATION_ID) + 1 FROM destination_db.dbo.CERTIFICATION")
            CERTIFICATION_ID = destination_cursor.fetchone()[0]

        destination_cursor.execute(insert_certification_query, (CERTIFICATION_ID, CONTACT_ID, row.LicenseNumber, row.LicenseType, 
                                                                row.LicenseStatus, create_date, issue_date, expiration_date))

    # Commit the transactions
    connection2.commit()


migrate_addresses()
migrate_licenses()


connection.close()
connection2.close()
source_cursor.close()
destination_cursor.close()
