# prism data access layer


This library can be used to interact with google big query or similar databases. 
If we are dealing with regular RDBMS/Nosql, we can direclty use
spring data templates/jpa repositories; we don't need to build a framework to handle it.


Right now, we will define the contracts (methods) and implementations for google query and leave the space for other dbs. It is open for extension.



In method implementations, we will try to cover all the boiler code implementation pertaining to google query. Client shouldn't be aware of the implementation. Client should call the contract and pass the necassary parameters (tablename and all).
		
		