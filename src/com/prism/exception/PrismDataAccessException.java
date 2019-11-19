package com.prism.exception;

import org.springframework.dao.DataAccessException;

public class PrismDataAccessException extends DataAccessException{
	
	
		public PrismDataAccessException(String msg) {
			super(msg);
		}

		public PrismDataAccessException(String msg, Throwable cause) {
			super(msg, cause);
		}
	}

