DROP TABLE IF EXISTS public.shipping_country_rates;
CREATE TABLE public.shipping_country_rates (
	shipping_country_id serial4 NOT NULL,
	shipping_country text NULL,
	shipping_country_base_rate numeric(14, 3) NULL,
	CONSTRAINT shipping_country_rates_pkey PRIMARY KEY (shipping_country_id)
);
DROP TABLE IF EXISTS public.shipping_agreement;
CREATE TABLE public.shipping_agreement (
	agreementid bigint NOT NULL,
	agreement_number text NULL,
	agreement_rate numeric(14, 3) NULL,
	agreement_commission numeric(14, 3) NULL, 
	CONSTRAINT shipping_agreement_pkey PRIMARY KEY (agreementid)
);
DROP TABLE IF EXISTS public.shipping_transfer;
CREATE TABLE public.shipping_transfer (
	transfer_type_id serial4 NOT NULL,
	transfer_type text NULL,
	transfer_model text NULL,
	shipping_transfer_rate  numeric(14, 3) NULL, 
	CONSTRAINT shipping_transfer_pkey PRIMARY KEY (transfer_type_id)
);
DROP TABLE IF EXISTS public.shipping_info;
CREATE TABLE public.shipping_info  (
	shippingid bigint NOT NULL,
	shipping_plan_datetime  timestamp NULL,
	payment_amount numeric(14, 2) NULL,
	vendorid int8 NULL,
	shipping_country_id int NOT NULL,
	transfer_type_id int NOT NULL,
	agreementid bigint NOT NULL,
	CONSTRAINT shipping_info_pkey PRIMARY KEY (shippingid),
	CONSTRAINT shipping_country_fkey FOREIGN KEY(shipping_country_id)
		REFERENCES public.shipping_country_rates(shipping_country_id),
	CONSTRAINT shipping_agreement_fkey FOREIGN KEY(agreementid)
		REFERENCES public.shipping_agreement(agreementid),
	CONSTRAINT shipping_transfer_fkey FOREIGN KEY(transfer_type_id)
		REFERENCES public.shipping_transfer(transfer_type_id)
);
DROP TABLE IF EXISTS public.shipping_status;
CREATE TABLE public.shipping_status (
	shippingid bigint NOT NULL,
	status  text NULL,
	state text NULL,
	shipping_start_fact_datetime  timestamp NULL,
	shipping_end_fact_datetime timestamp NULL,
	CONSTRAINT shipping_status_pkey PRIMARY KEY (shippingid)
);
