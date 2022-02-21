prepare_db_sql = """
CREATE TABLE public.campaigns (
	campaign_id int8 NOT NULL,
	structure_value varchar NOT NULL,
	status varchar NOT NULL,
	CONSTRAINT campaigns_pkey PRIMARY KEY (campaign_id)
);

CREATE TABLE public.adgroups (
	ad_group_id int8 NOT NULL,
	campaign_id int8 NOT NULL,
	alias varchar NOT NULL,
	status varchar NOT NULL,
	CONSTRAINT adgroups_pkey PRIMARY KEY (ad_group_id),
	CONSTRAINT campaign_id FOREIGN KEY (campaign_id) REFERENCES public.campaigns(campaign_id)
);

CREATE TABLE public.search_terms (
	"date" date NOT NULL,
	ad_group_id int8 NOT NULL,
	campaign_id int8 NOT NULL,
	clicks int4 NULL,
	"cost" numeric NULL,
	conversion_value numeric NULL,
	conversions int4 NULL,
	search_term varchar NOT NULL,
	CONSTRAINT search_term_id PRIMARY KEY (ad_group_id, campaign_id, search_term, date),
	CONSTRAINT ad_group_id FOREIGN KEY (ad_group_id) REFERENCES public.adgroups(ad_group_id),
	CONSTRAINT campaign_id FOREIGN KEY (campaign_id) REFERENCES public.campaigns(campaign_id)
);
"""