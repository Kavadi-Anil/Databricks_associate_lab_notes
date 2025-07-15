-- Databricks notebook source
CREATE CATALOG IF NOT EXISTS democatalog;
USE CATALOG democatalog;

CREATE SCHEMA IF NOT EXISTS dbdemos;
USE SCHEMA dbdemos;

CREATE VOLUME IF NOT EXISTS democatalog.dbdemos.raw_data;
CREATE VOLUME IF NOT EXISTS democatalog.dbdemos.users_raw_data;

CREATE OR REPLACE TABLE users(ID INT);
CREATE OR REPLACE TABLE sales(SALESID INT);

CREATE VIEW IF NOT EXISTS sales_by_user_vw 
AS 
SELECT * 
FROM sales;


CREATE OR REPLACE FUNCTION democatalog.dbdemos.clean_sales_data(number FLOAT) 
RETURNS FLOAT
RETURN number * number;