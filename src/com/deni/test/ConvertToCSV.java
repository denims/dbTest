package com.deni.test;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.ScanOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.ScanSpec;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;

public class ConvertToCSV {

	public static void main(String[] args) {

		// DynamoDB

		AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().withEndpointConfiguration(
				new AwsClientBuilder.EndpointConfiguration("http://localhost:8000", "ap-south-1")).build();

		DynamoDB dynamoDB = new DynamoDB(client);
		Table table = dynamoDB.getTable("Employee");
		// Declaring the scanspec
		ScanSpec scanSpec = new ScanSpec();

		List<EmpDetails> output = new ArrayList<EmpDetails>();

		EmpDetails details;
		try {
			ItemCollection<ScanOutcome> items = table.scan(scanSpec);

			Iterator<Item> iter = items.iterator();
			while (iter.hasNext()) {
				Item item = iter.next();
				details = new EmpDetails();
				details.empName = item.getString("Name");
				details.empDesig = item.getString("Designation");
				output.add(details);

			}
			// System.out.println(items.toString());

		} catch (Exception e) {
			System.err.println("Unable to scan the table:");
			System.err.println(e.getMessage());
		}

		// create mapper and schema for csv
		CsvMapper mapper = new CsvMapper();
		CsvSchema schema = mapper.schemaFor(EmpDetails.class);
		schema = schema.withColumnSeparator('\t');

		// output writer
		try {
			ObjectWriter myObjectWriter = mapper.writer(schema);
			File tempFile = new File("users.csv");
			FileOutputStream tempFileOutputStream = new FileOutputStream(tempFile);
			BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(tempFileOutputStream, 1024);
			OutputStreamWriter writerOutputStream = new OutputStreamWriter(bufferedOutputStream, "UTF-8");
			myObjectWriter.writeValue(writerOutputStream, output);
			System.out.println("Completed");
		} catch (Exception e) {
			System.err.println(e.getMessage());
		}
	}

	// Class for Employee details
	@JsonPropertyOrder({ "empName", "empDesig" })
	static class EmpDetails {

		public EmpDetails() {
			super();
		}

		public String empName;
		public String empDesig;

		public String getEmpName() {
			return empName;
		}

		public void setEmpName(String empName) {
			this.empName = empName;
		}

		public String getEmpDesig() {
			return empDesig;
		}

		public void setEmpDesig(String empDesig) {
			this.empDesig = empDesig;
		}

	}

	public EmpDetails newEmp() {
		return new EmpDetails();
	}

}
