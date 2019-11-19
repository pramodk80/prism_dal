package com.prism.dal.operations;

import java.io.InputStream;

import org.springframework.util.concurrent.ListenableFuture;

import com.google.cloud.bigquery.FormatOptions;

public interface DataRepository {

		
		
		/**
		 * To write into a big query table
		 * @param tableName
		 * @param inputStream
		 * @param dataFormatOptions
		 * @return
		 */
		ListenableFuture<com.google.cloud.bigquery.Job> writeDataToTable(
				String tableName, InputStream inputStream, FormatOptions dataFormatOptions);
		
		//TODO: other methods need to be defined.
		
		// 1.create dynamic table.
		// 2. Read from table.
		// 3. Delete data from table.
		// 4. delete table.
		
		

}
