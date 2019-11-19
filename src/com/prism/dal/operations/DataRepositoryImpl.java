package com.prism.dal.operations;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.time.Duration;
import java.util.concurrent.ScheduledFuture;

import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.concurrent.DefaultManagedTaskScheduler;
import org.springframework.util.Assert;
import org.springframework.util.StreamUtils;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.SettableListenableFuture;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.FormatOptions;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobInfo.WriteDisposition;
import com.google.cloud.bigquery.JobStatus.State;
import com.google.cloud.bigquery.TableDataWriteChannel;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.WriteChannelConfiguration;
import com.prism.exception.PrismDataAccessException;
public class DataRepositoryImpl implements DataRepository{

	private final BigQuery bigQuery;

	private final String datasetName;

	private final TaskScheduler taskScheduler;

	private boolean autoDetectSchema = true;

	private WriteDisposition writeDisposition = WriteDisposition.WRITE_APPEND;

	private Duration jobPollInterval = Duration.ofSeconds(2);

	/**
	 * Creates the {@link BigQuery} template.
	 *
	 * @param bigQuery the underlying client object used to interface with BigQuery
	 * @param datasetName the name of the dataset in which all operations will take place
	 */
	public DataRepositoryImpl(BigQuery bigQuery, String datasetName) {
		this(bigQuery, datasetName, new DefaultManagedTaskScheduler());
	}

	/**
	 * Creates the {@link BigQuery} template.
	 *
	 * @param bigQuery the underlying client object used to interface with BigQuery
	 * @param datasetName the name of the dataset in which all operations will take place
	 * @param taskScheduler the {@link TaskScheduler} used to poll for the status of
	 *     long-running BigQuery operations
	 */
	public DataRepositoryImpl(BigQuery bigQuery, String datasetName, TaskScheduler taskScheduler) {
		Assert.notNull(bigQuery, "BigQuery client object must not be null.");
		Assert.notNull(datasetName, "Dataset name must not be null");
		Assert.notNull(taskScheduler, "TaskScheduler must not be null");

		this.bigQuery = bigQuery;
		this.datasetName = datasetName;
		this.taskScheduler = taskScheduler;
	}

	/**
	 * Sets whether BigQuery should attempt to autodetect the schema of the data when loading
	 * data into an empty table for the first time. If set to false, the schema must be
	 * defined explicitly for the table before load.
	 * @param autoDetectSchema whether data schema should be autodetected from the structure
	 *     of the data. Default is true.
	 */
	public void setAutoDetectSchema(boolean autoDetectSchema) {
		this.autoDetectSchema = autoDetectSchema;
	}

	/**
	 * Sets the {@link WriteDisposition} which specifies how data should be inserted into
	 * BigQuery tables.
	 * @param writeDisposition whether to append to or truncate (overwrite) data in the
	 *     BigQuery table. Default is {@code WriteDisposition.WRITE_APPEND} to append data to
	 *     a table.
	 */
	public void setWriteDisposition(WriteDisposition writeDisposition) {
		Assert.notNull(writeDisposition, "BigQuery write disposition must not be null.");
		this.writeDisposition = writeDisposition;
	}

	/**
	 * Sets the {@link Duration} amount of time to wait between successive polls on the status
	 * of a BigQuery job.
	 * @param jobPollInterval the {@link Duration} poll interval for BigQuery job status
	 *     polling
	 */
	public void setJobPollInterval(Duration jobPollInterval) {
		Assert.notNull(jobPollInterval, "BigQuery job polling interval must not be null");
		this.jobPollInterval = jobPollInterval;
	}

	@Override
	public ListenableFuture<Job> writeDataToTable(
			String tableName, InputStream inputStream, FormatOptions dataFormatOptions) {
		TableId tableId = TableId.of(datasetName, tableName);

		WriteChannelConfiguration writeChannelConfiguration = WriteChannelConfiguration
				.newBuilder(tableId)
				.setFormatOptions(dataFormatOptions)
				.setAutodetect(this.autoDetectSchema)
				.setWriteDisposition(this.writeDisposition)
				.build();

		TableDataWriteChannel writer = bigQuery.writer(writeChannelConfiguration);

		try (OutputStream sink = Channels.newOutputStream(writer)) {
			// Write data from data input file to BigQuery
			StreamUtils.copy(inputStream, sink);
		}
		catch (IOException e) {
			throw new PrismDataAccessException("Failed to write data to BigQuery tables.", e);
		}

		if (writer.getJob() == null) {
			throw new PrismDataAccessException(
					"Failed to initialize the BigQuery write job.");
		}

		return createJobFuture(writer.getJob());
	}

	/**
	 * @return the name of the BigQuery dataset that the template is operating in.
	 */
	public String getDatasetName() {
		return this.datasetName;
	}

	private SettableListenableFuture<Job> createJobFuture(Job pendingJob) {
		// Prepare the polling task for the ListenableFuture result returned to end-user
		SettableListenableFuture<Job> result = new SettableListenableFuture<>();

		ScheduledFuture<?> scheduledFuture = taskScheduler.scheduleAtFixedRate(() -> {
			Job job = pendingJob.reload();
			if (State.DONE.equals(job.getStatus().getState())) {
				if (job.getStatus().getError() != null) {
					result.setException(
							new PrismDataAccessException(job.getStatus().getError().getMessage()));
				}
				else {
					result.set(job);
				}
			}
		}, this.jobPollInterval);

		result.addCallback(
				response -> scheduledFuture.cancel(true),
				response -> {
					pendingJob.cancel();
					scheduledFuture.cancel(true);
				});

		return result;
	}
}
