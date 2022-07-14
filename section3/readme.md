## Section 3: System Design

You are designing data infrastructure on the cloud for a company whose main business is in processing images.

The company has a web application which collects images uploaded by customers. The company also has a separate web application which provides a stream of images using a Kafka stream. The company’s software engineers have already some code written to process the images. The company would like to save processed images for a minimum of 7 days for archival purposes. Ideally, the company would also want to be able to have some Business Intelligence (BI) on key statistics including number and type of images processed, and by which customers.

Produce a system architecture diagram (e.g. Visio, Powerpoint) using any of the commercial cloud providers' ecosystem to explain your design. Please also indicate clearly if you have made any assumptions at any point.

---

### Assumptions

- The developed software for processing has exposed APIs for usage, and will be interfaced from pipeline as a black box. It will not be run serverless, hence no requirement to develop ec2 compute cluster in the processing layer. 

- A skeletal Data warehousing system for simple BI is provided. No extra metadata will be captured beyond what is available from S3 to reduce components to maintain. No OLTP layer is provided.

- Additional visualization is provided, and an optional Audit table store in data warehouse (Redshift) is also possible if deemed necessary. 
