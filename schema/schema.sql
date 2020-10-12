CREATE TABLE country(
  country_id INTEGER,
  country_name VARCHAR,
  PRIMARY KEY (country_id ASC)
);

CREATE TABLE university(
  university_id INTEGER,
  university_name VARCHAR,
  country_id INTEGER,
  PRIMARY KEY (university_id ASC),
  FOREIGN KEY (country_id) REFERENCES country(country_id) ON UPDATE CASCADE ON DELETE RESTRICT
);
CREATE INDEX university_name_index ON university(university_name);

CREATE TABLE mathematics_subject_classification(
  subject_code INTEGER NOT NULL,
  subject_name VARCHAR NOT NULL,
  PRIMARY KEY (subject_code)
);

CREATE TABLE dissertation(
  dissertation_id INTEGER,
  dissertation_title VARCHAR,
  dissertation_year INTEGER,
  subject_code INTEGER,
  university_id INTEGER,
  FOREIGN KEY (university_id) REFERENCES university(university_id) ON UPDATE CASCADE ON DELETE RESTRICT
  PRIMARY KEY (dissertation_id ASC),
  FOREIGN KEY (subject_code) REFERENCES mathematics_subject_classification(subject_code) ON UPDATE CASCADE ON DELETE RESTRICT
);
CREATE INDEX dissertation_year_index ON dissertation(dissertation_year);

CREATE TABLE mathematician(
  mathematician_id INTEGER NOT NULL,
  mathematician_name VARCHAR NOT NULL,
  birth_date DATE,
  death_date DATE,
  dissertation_id INTEGER,
  PRIMARY KEY (mathematician_id),
  FOREIGN KEY (dissertation_id) REFERENCES dissertation(dissertation_id) ON UPDATE CASCADE ON DELETE RESTRICT
);
CREATE INDEX mathematician_birth_date_index ON mathematician(birth_date);
CREATE INDEX mathematician_death_date_index ON mathematician(death_date);

CREATE TABLE advisor_relationship(
  advisor_id INTEGER NOT NULL,
  advisee_id INTEGER NOT NULL,
  FOREIGN KEY (advisor_id) REFERENCES mathematician(mathematician_id) ON UPDATE CASCADE ON DELETE RESTRICT,
  FOREIGN KEY (advisee_id) REFERENCES mathematician(mathematician_id) ON UPDATE CASCADE ON DELETE RESTRICT,
);

CREATE TABLE web_source(
  web_source_id INTEGER,
  base_url VARCHAR NOT NULL UNIQUE,
  PRIMARY KEY (web_source_id ASC)
);

CREATE TABLE webpage(
  webpage_id INTEGER,
  web_source_id INTEGER NOT NULL,
  path VARCHAR,
  query VARCHAR,
  timestamp DATE,
  PRIMARY KEY (webpage_id ASC),
  FOREIGN KEY (web_source_id) REFERENCES web_source(web_source_id) ON UPDATE CASCADE ON DELETE RESTRICT
);

CREATE TABLE math_genealogy_associated_link(
  mathematician_id INTEGER NOT NULL,
  webpage_id INTEGER NOT NULL,
  href_text VARCHAR,
  FOREIGN KEY (mathematician_id) REFERENCES mathematician(mathematician_id) ON UPDATE CASCADE ON DELETE CASCADE,
  FOREIGN KEY (webpage_id) REFERENCES webpage(webpage_id) ON UPDATE CASCADE ON DELETE RESTRICT
);
