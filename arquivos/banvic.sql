--
-- PostgreSQL database dump
--

-- Dumped from database version 16.9 (Ubuntu 16.9-1.pgdg22.04+1)
-- Dumped by pg_dump version 16.9 (Ubuntu 16.9-1.pgdg22.04+1)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: status_proposta; Type: TYPE; Schema: public; Owner: -
--

\c banvic

CREATE TYPE public.status_proposta AS ENUM (
    'Enviada',
    'Validação documentos',
    'Aprovada',
    'Reprovada',
    'Em análise'
);


--
-- Name: tipo_agencia; Type: TYPE; Schema: public; Owner: -
--

CREATE TYPE public.tipo_agencia AS ENUM (
    'Digital',
    'Física'
);


--
-- Name: tipo_cliente; Type: TYPE; Schema: public; Owner: -
--

CREATE TYPE public.tipo_cliente AS ENUM (
    'PF',
    'PJ'
);


SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: agencias; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.agencias (
    cod_agencia integer NOT NULL,
    nome character varying(255) NOT NULL,
    endereco text,
    cidade character varying(255),
    uf character(2),
    data_abertura date,
    tipo_agencia public.tipo_agencia
);


--
-- Name: clientes; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.clientes (
    cod_cliente integer NOT NULL,
    primeiro_nome character varying(255) NOT NULL,
    ultimo_nome character varying(255) NOT NULL,
    email character varying(255) NOT NULL,
    tipo_cliente public.tipo_cliente,
    data_inclusao timestamp with time zone,
    cpfcnpj character varying(18) NOT NULL,
    data_nascimento date,
    endereco text,
    cep character varying(9)
);


--
-- Name: colaborador_agencia; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.colaborador_agencia (
    cod_colaborador integer NOT NULL,
    cod_agencia integer NOT NULL
);


--
-- Name: colaboradores; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.colaboradores (
    cod_colaborador integer NOT NULL,
    primeiro_nome character varying(255) NOT NULL,
    ultimo_nome character varying(255) NOT NULL,
    email character varying(255) NOT NULL,
    cpf character varying(14) NOT NULL,
    data_nascimento date,
    endereco text,
    cep character varying(9)
);


--
-- Name: contas; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.contas (
    num_conta bigint NOT NULL,
    cod_cliente integer,
    cod_agencia integer,
    cod_colaborador integer,
    tipo_conta public.tipo_cliente,
    data_abertura timestamp with time zone,
    saldo_total numeric(15,2),
    saldo_disponivel numeric(15,2),
    data_ultimo_lancamento timestamp with time zone
);


--
-- Name: propostas_credito; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.propostas_credito (
    cod_proposta integer NOT NULL,
    cod_cliente integer,
    cod_colaborador integer,
    data_entrada_proposta timestamp with time zone,
    taxa_juros_mensal numeric(5,4),
    valor_proposta numeric(15,2),
    valor_financiamento numeric(15,2),
    valor_entrada numeric(15,2),
    valor_prestacao numeric(15,2),
    quantidade_parcelas integer,
    carencia integer,
    status_proposta public.status_proposta
);


--
-- Data for Name: agencias; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.agencias (cod_agencia, nome, endereco, cidade, uf, data_abertura, tipo_agencia) FROM stdin;
7	Agência Digital	Av. Paulista, 1436 - Cerqueira César, São Paulo - SP, 01310-916	São Paulo	SP	2015-08-01	Digital
1	Agência Matriz	Av. Paulista, 1436 - Cerqueira César, São Paulo - SP, 01310-916	São Paulo	SP	2010-01-01	Física
2	Agência Tatuapé	Praça Sílvio Romero, 158 - Tatuapé, São Paulo - SP, 03323-000	São Paulo	SP	2010-06-14	Física
3	Agência Campinas	Av. Francisco Glicério, 895 - Vila Lidia, Campinas - SP, 13012-000	Campinas	SP	2012-03-04	Física
4	Agência Osasco	Av. Antônio Carlos Costa, 1000 - Bela Vista, Osasco - SP, 06053-014	Osasco	SP	2013-11-06	Física
5	Agência Porto Alegre	Av. Bento Gonçalves, 1924 - Partenon, Porto Alegre - RS, 90650-000	Porto Alegre	RS	2013-12-01	Física
6	Agência Rio de Janeiro	R. Sen. Dantas, 15 - Centro, Rio de Janeiro - RJ, 20031-202	Rio de Janeiro	RJ	2015-04-01	Física
8	Agência Jardins	Av. Brg. Faria Lima, 2491 - Jardim Paulistano, São Paulo - SP, 01452-000	São Paulo	SP	2018-01-09	Física
9	Agência Florianópolis	Av. Jorn. Rubéns de Arruda Ramos, 1280 - Centro, Florianópolis - SC, 88015-700	Florianópolis	SC	2019-10-09	Física
10	Agência Recife	Av. Conselheiro Aguiar, 4432 - Boa Viagem, Recife - PE, 51021-020	Recife	PE	2021-10-09	Física
\.


--
-- Data for Name: clientes; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.clientes (cod_cliente, primeiro_nome, ultimo_nome, email, tipo_cliente, data_inclusao, cpfcnpj, data_nascimento, endereco, cep) FROM stdin;
28	Sabrina	Dias	moreiraemanuelly@example.org	PF	2017-04-03 13:11:00-03	357.081.496-39	2006-08-11	Praia de Duarte Vila Piratininga 81327-166 Fernandes das Pedras / SP	95140-704
674	Luiz Felipe	Dias	pedroferreira@example.org	PF	2021-02-10 10:27:00-03	085.196.374-93	1995-10-11	Avenida da Rosa, 654 João Paulo Ii 20295449 Nunes / AP	76516-765
693	Renan	Dias	ogomes@example.net	PF	2020-01-21 10:12:00-03	783.416.059-10	1948-11-19	Jardim de Rodrigues Ipiranga 14161-477 Duarte da Praia / RN	51779625
743	Clara	Dias	rafaelcorreia@example.org	PF	2019-05-06 08:39:00-03	589.237.610-95	1978-01-27	Colônia Thomas Silva, 9 Tupi B 15771-946 Fernandes / SP	19615792
769	André	Dias	danilo33@example.org	PF	2017-01-07 12:53:00-02	459.608.721-02	1990-08-25	Rua Correia, 889 Diamante 59123250 Aragão / RS	01672838
824	Heloísa	Dias	alanadias@example.org	PF	2016-01-07 10:26:00-02	536.981.240-33	1967-05-12	Favela de da Mata, 3 Conjunto Novo Dom Bosco 77145256 Vieira / MS	07814-131
884	Maria Julia	Dias	alvesmariana@example.net	PF	2017-08-06 08:01:00-03	150.438.926-33	1950-12-10	Esplanada Maria Luiza Ribeiro, 4 Conjunto Califórnia Ii 13748392 Moraes / ES	90221-380
899	Arthur	Dias	castropedro-lucas@example.net	PF	2021-10-17 10:00:00-03	509.481.762-02	1953-02-26	Ladeira Elisa da Rocha, 69 João Pinheiro 13964-513 Araújo da Prata / PB	44875-244
908	Vitor Gabriel	Dias	acardoso@example.com	PF	2021-04-02 07:22:00-03	158.406.379-39	1948-03-25	Passarela Lucca Castro, 73 Vila Paris 99268696 Farias / BA	42603-833
945	Caio	Dias	moraesmarina@example.org	PF	2016-05-10 10:56:00-03	975.860.431-75	1955-06-11	Fazenda Emanuel Gomes, 6 Cruzeiro 29428-910 Novaes / SP	23176352
5	Daniela	Lima	yago42@example.net	PF	2015-01-14 09:53:00-02	071.259.643-70	1994-03-04	Lago de Moraes, 592 Mirtes 10935-233 Nascimento de Peixoto / AL	96027-142
262	João Lucas	Lima	brunaduarte@example.org	PF	2019-10-19 08:14:00-03	358.702.964-47	1950-10-17	Rua João Lucas Freitas, 46 Vila Nova Cachoeirinha 2ª Seção 97602-589 Castro de Goiás / AC	29942-354
281	Enrico	Lima	fernandesbryan@example.net	PF	2016-06-27 12:06:00-03	180.746.392-31	1978-01-15	Praia Isabelly Martins, 7 Morro Dos Macacos 27993489 da Mota do Amparo / MS	97628-818
352	Carolina	Lima	maria-sophiacardoso@example.com	PF	2022-06-05 11:12:00-03	658.079.314-75	1948-07-19	Trecho de Campos, 54 Cônego Pinheiro 1ª Seção 77765-955 Novaes / AP	57113-154
387	Isaac	Lima	nicolecardoso@example.com	PF	2014-02-08 11:28:00-02	869.103.725-30	1945-07-22	Chácara Pietra Nascimento, 7 Apolonia 81600-416 Novaes das Pedras / AL	26119706
522	João Lucas	Lima	barbosamariana@example.org	PF	2016-11-05 11:26:00-02	740.963.251-80	1990-05-05	Avenida Mariana Rezende, 42 Fernão Dias 25175-265 Ramos / DF	80317732
729	Benjamin	Lima	kaiquepeixoto@example.net	PF	2020-08-21 07:13:00-03	095.421.876-02	1956-08-05	Avenida Luna Correia, 67 Outro 07065604 Correia / SP	77226042
887	Kevin	Lima	kamilly26@example.net	PF	2021-02-27 08:43:00-03	538.169.047-93	1950-07-09	Passarela Silva, 4 Vila Nova Cachoeirinha 2ª Seção 92334096 Alves do Oeste / ES	12716-003
70	Enrico	Melo	pedro-henriqueviana@example.org	PF	2021-02-18 12:23:00-03	418.962.035-15	1988-11-08	Esplanada Rezende, 82 Vila Minaslandia 86011416 Fernandes / SC	37988-526
226	Isabella	Melo	vramos@example.com	PF	2017-01-14 09:27:00-02	716.452.809-85	1952-10-01	Viela Theo Ramos, 14 Paquetá 59350-231 Pereira da Serra / SC	16954952
239	Bernardo	Melo	maria-alice61@example.net	PF	2017-02-25 13:31:00-03	327.459.861-55	1979-04-26	Fazenda Gomes, 81 Cidade Jardim Taquaril 81318-214 da Rocha do Galho / PE	51092202
265	Heitor	Melo	vitor-gabrielmonteiro@example.com	PF	2019-06-05 08:17:00-03	598.067.321-03	1989-10-25	Vila Gonçalves, 11 Pousada Santo Antonio 46466-651 Ferreira / GO	96975888
366	Pedro	Melo	arthurvieira@example.com	PF	2014-06-06 12:19:00-03	809.461.527-30	1978-03-21	Lago de Barros, 8 Sport Club 21887406 Monteiro das Flores / GO	73739-486
467	Gustavo Henrique	Melo	da-cunhabrenda@example.com	PF	2018-02-26 09:52:00-03	532.709.841-97	1999-10-12	Colônia de Ribeiro, 720 Ademar Maldonado 84104-262 Pereira / ES	50832-820
602	Thomas	Melo	mda-rocha@example.net	PF	2018-10-24 10:07:00-03	549.671.082-02	1991-08-25	Vila de Pereira, 48 Mantiqueira 16617426 Cardoso / BA	77360-182
691	Raquel	Melo	thiago84@example.net	PF	2020-07-15 13:46:00-03	874.351.602-53	1978-08-21	Sítio Enzo Fernandes Madre Gertrudes 07858527 da Cruz Verde / AP	51214338
783	Nicole	Melo	ericknogueira@example.com	PF	2015-09-21 08:35:00-03	602.143.987-22	1968-07-13	Sítio Teixeira, 99 Conjunto Jatoba 48852-845 Correia de Goiás / PI	49990438
786	Luna	Melo	oliveiradavi@example.net	PF	2019-01-21 12:29:00-02	125.386.974-09	1959-05-16	Morro Kaique Fogaça, 5 Barão Homem De Melo 2ª Seção 99410668 Fernandes / RN	84504002
792	Levi	Melo	da-cunhajoao-guilherme@example.net	PF	2018-12-08 10:54:00-02	061.947.285-58	1977-01-11	Vereda Vieira, 835 Baleia 98810617 Oliveira da Prata / BA	63158375
811	Yuri	Melo	mouralivia@example.com	PF	2018-08-06 08:04:00-03	943.162.708-69	1972-11-13	Estrada Nicole Moraes, 36 Vila Petropolis 83982453 Alves / TO	69025958
834	Luna	Melo	caldeiranicolas@example.com	PF	2015-03-25 11:02:00-03	416.287.395-09	1967-09-20	Vila Fernandes, 242 Vila Satélite 62724113 Fernandes da Mata / RJ	05254-597
911	Daniel	Melo	nicole57@example.com	PF	2011-09-22 09:17:00-03	576.184.902-01	1984-03-23	Vale Antônio Lima, 4 São João 47295-375 Nunes / PA	04065275
924	Melissa	Melo	samuelmonteiro@example.com	PF	2021-10-20 12:55:00-03	412.307.695-52	1988-07-08	Trecho Daniel Freitas Vila Dos Anjos 03780697 Nascimento / PB	47546-871
993	Beatriz	Melo	brendacaldeira@example.org	PF	2017-10-07 08:40:00-03	786.914.320-04	1998-03-16	Parque Rodrigues, 104 Nova America 72291173 Peixoto / TO	88422-547
56	Maria Luiza	Alves	souzasophie@example.org	PF	2022-09-21 11:40:00-03	428.691.053-98	1992-09-08	Loteamento Campos, 6 Dona Clara 75778-811 Pires das Flores / SP	87868-132
71	Luiza	Alves	sdias@example.com	PF	2013-12-14 11:59:00-02	578.962.410-30	1954-12-31	Via Augusto Moura, 59 Vila Independencia 2ª Seção 01782369 Vieira Grande / AL	67750-393
92	Daniel	Alves	carvalhofernando@example.com	PF	2017-02-03 13:31:00-02	923.540.671-43	1969-11-22	Alameda Maria Clara Lopes, 48 Alto Barroca 93067-803 Ramos da Serra / RJ	30314554
116	Isaac	Alves	fmoraes@example.com	PF	2014-10-17 10:19:00-03	783.290.516-68	1953-08-01	Setor Silveira, 9 Esperança 54390-541 Cavalcanti / PB	75462082
289	Maria Alice	Alves	renanmoura@example.net	PF	2022-12-11 11:34:00-03	826.703.591-59	1983-02-14	Lago da Cunha São Francisco Das Chagas 39623597 da Cunha / MS	11965-789
364	Miguel	Alves	bcarvalho@example.net	PF	2013-09-07 11:06:00-03	806.751.439-93	1967-07-30	Favela Mendes, 15 Cônego Pinheiro 1ª Seção 48858415 Ramos / PI	14157-016
381	Melissa	Alves	oliveiracecilia@example.net	PF	2016-10-21 13:50:00-02	753.481.029-97	1972-11-01	Parque Lucca Farias, 16 Vila Madre Gertrudes 3ª Seção 09192-662 da Rosa de da Cruz / RO	26941177
421	Mirella	Alves	luna28@example.com	PF	2020-04-24 07:07:00-03	903.648.527-47	1970-11-09	Loteamento Rodrigues, 869 Palmares 97692-838 da Cunha / AM	88078064
664	Clara	Alves	oliviasantos@example.org	PF	2017-11-12 13:21:00-02	421.603.795-06	1975-06-12	Distrito Ferreira, 19 São Luiz 77702611 Nascimento do Norte / RS	02434042
788	Lavínia	Alves	diogoramos@example.org	PF	2021-06-11 08:11:00-03	742.059.863-00	1954-01-02	Condomínio de Duarte, 4 Alta Tensão 2ª Seção 90644-133 Pereira / PA	25512578
829	Sarah	Alves	rauldas-neves@example.com	PF	2020-04-16 07:33:00-03	274.803.516-07	1946-09-06	Distrito de da Mota, 41 Aeroporto 74948002 da Cunha / PI	98200059
847	Thiago	Alves	bda-rocha@example.com	PF	2017-10-23 10:25:00-02	539.186.704-57	1976-09-25	Viaduto Laura Pinto, 5 Vila Santa Monica 1ª Seção 93180-322 da Rosa / SP	31500163
852	Isabelly	Alves	joao-lucas08@example.com	PF	2015-07-01 13:05:00-03	416.739.085-00	1983-10-20	Trecho de Barbosa, 9 Lagoinha Leblon 56609-518 Ferreira / ES	18876-575
991	Luiz Miguel	Alves	luana66@example.com	PF	2019-02-14 11:36:00-02	587.609.124-30	1964-10-26	Chácara de Moreira, 60 Olaria 22714-326 Ribeiro da Mata / AM	86294-895
90	João	Costa	nina62@example.com	PF	2014-05-15 10:06:00-03	926.057.138-30	1947-08-21	Feira de Teixeira, 4 Laranjeiras 89122-999 Pires / SP	38071-009
233	Catarina	Costa	joao-felipecorreia@example.org	PF	2014-12-02 12:14:00-02	603.751.829-77	1961-12-03	Passarela de da Conceição, 2 Centro 93490-179 Moraes de Oliveira / AL	38180255
356	Cecília	Costa	emilly14@example.org	PF	2014-01-13 09:30:00-02	092.813.567-59	1950-11-13	Lago de Jesus Esperança 91251916 da Cruz / SP	89813224
493	Laís	Costa	silveiralavinia@example.com	PF	2017-12-19 14:53:00-02	867.402.159-01	1992-05-08	Parque Moura Parque São José 91515-389 Nascimento / MG	98259-386
676	Maria Vitória	Costa	rochanina@example.org	PF	2016-04-25 12:44:00-03	182.954.037-88	1955-11-10	Jardim da Rocha, 65 Minaslandia 48773387 Araújo / BA	67484257
890	Maysa	Costa	ida-rosa@example.org	PF	2013-12-11 12:23:00-02	286.903.175-03	2001-06-17	Colônia Ferreira, 1 Suzana 76789911 Gomes da Praia / MT	07873-123
957	Beatriz	Costa	nascimentobruno@example.org	PF	2020-08-10 12:07:00-03	627.983.405-00	1974-10-21	Travessa de Sales, 37 Jardim América 26138-341 da Luz / PA	56424-819
38	Augusto	Cunha	cda-cunha@example.net	PF	2021-08-23 09:15:00-03	619.284.305-89	1984-07-01	Residencial de Vieira, 57 Conjunto Serra Verde 85249271 Rocha / MG	81473-142
55	Sophie	Cunha	oliveiravitor@example.com	PF	2016-11-10 12:25:00-02	521.934.708-04	1970-05-16	Passarela de Costela Senhor Dos Passos 66807-828 da Rocha / RJ	19744436
150	Pedro Lucas	Cunha	jesusjulia@example.com	PF	2021-11-04 08:27:00-03	385.417.629-55	2002-04-14	Distrito de Cardoso, 66 Vila Inestan 46530-625 Costela / ES	74907782
211	João Miguel	Cunha	davi-lucas04@example.net	PF	2021-03-10 09:41:00-03	351.678.904-20	1942-10-30	Praça de Campos, 98 São Salvador 91131699 Nunes / MT	07845-704
331	Vitória	Cunha	lopesemanuel@example.com	PF	2012-09-22 13:29:00-03	187.024.695-02	1967-01-15	Distrito de Gonçalves Inconfidência 55785823 da Mota de Moura / SP	11168371
452	Kevin	Cunha	dmonteiro@example.com	PF	2020-07-18 08:22:00-03	128.495.703-97	1954-12-27	Travessa de Cardoso Nossa Senhora Da Conceição 41836-383 Dias Alegre / SC	43803139
587	Mariana	Cunha	maria-eduarda57@example.org	PF	2019-09-06 12:59:00-03	903.251.768-68	1992-08-13	Campo Stella Ferreira Novo Ouro Preto 12789-557 Pires / PI	50946020
649	Clarice	Cunha	brenda34@example.net	PF	2016-04-11 10:35:00-03	631.528.049-89	1958-10-02	Viela Thales Santos, 133 Vila Santa Monica 2ª Seção 08043-756 Almeida Paulista / MT	37183-556
663	João Pedro	Cunha	carlos-eduardoda-luz@example.org	PF	2020-07-21 11:21:00-03	068.235.197-03	1977-06-17	Via Ferreira Vila Dos Anjos 28261187 Castro / RN	03481410
720	Lara	Cunha	t2homasbarbosa@example.com	PF	2021-03-23 07:56:00-03	780.629.134-22	1955-12-14	Setor de Rodrigues, 46 Santa Margarida 46508-985 Campos de Goiás / ES	70396-535
741	Gustavo	Cunha	correiaemanuelly@example.org	PF	2012-03-03 11:17:00-03	237.604.598-38	1955-11-13	Favela Luiz Otávio Santos Tupi B 43588859 Rezende da Mata / SC	77732-400
780	Thiago	Cunha	carvalhonicolas@example.com	PF	2018-02-05 09:35:00-02	352.748.691-73	1981-04-11	Avenida Gonçalves, 38 Ademar Maldonado 06852555 Ribeiro Alegre / PE	99956563
866	Brenda	Cunha	qcaldeira@example.net	PF	2019-06-22 10:36:00-03	936.475.821-82	1942-07-26	Esplanada Levi Barbosa, 216 Vila São João Batista 50276-230 Silva / GO	43609-121
933	Leonardo	Cunha	aragaolorena@example.net	PF	2015-08-12 10:49:00-03	739.416.528-00	2003-11-20	Avenida Cunha, 13 Flavio Marques Lisboa 36827006 Caldeira de Martins / ES	25715953
941	Otávio	Cunha	carvalhonicole@example.org	PF	2022-05-22 09:02:00-03	310.842.695-89	1963-02-07	Viela Farias, 926 Heliopolis 61000-447 Barros Grande / SE	20775-963
68	Lorena	Gomes	nrezende@example.net	PF	2019-08-11 08:02:00-03	832.607.514-08	1955-10-09	Rodovia Alexia Costela, 374 Vila Madre Gertrudes 4ª Seção 35410645 Melo / PA	13577256
79	Milena	Gomes	bsilva@example.com	PF	2022-12-03 07:51:00-03	893.726.041-78	1980-02-11	Alameda Benício Sales, 68 São Sebastião 87879-290 da Rocha da Mata / PA	31297275
157	Emanuella	Gomes	antoniosilva@example.com	PF	2017-08-20 09:13:00-03	659.284.301-24	2006-05-29	Vale Santos Vila Mantiqueira 16535-555 Lopes / MT	45442696
195	Luiz Fernando	Gomes	vcastro@example.net	PF	2011-03-08 10:26:00-03	046.853.129-70	1967-11-28	Largo de Sales, 95 Lindéia 03767449 Aragão / GO	13895327
369	João Lucas	Gomes	lribeiro@example.net	PF	2022-10-09 08:31:00-03	256.389.041-15	1965-04-29	Chácara Cunha, 551 Serra 26207-436 Silveira / CE	27315-383
499	Helena	Gomes	fda-mota@example.org	PF	2022-11-07 08:37:00-03	503.742.189-60	2002-01-24	Loteamento de Vieira, 36 Vila Nossa Senhora Do Rosário 99556-419 da Conceição do Oeste / SP	58899466
567	Bryan	Gomes	hrezende@example.com	PF	2011-05-16 07:17:00-03	619.520.487-02	1997-04-06	Núcleo de Freitas, 378 Miramar 12606621 Correia / ES	34609558
609	Larissa	Gomes	ana-laura55@example.org	PF	2017-05-14 12:03:00-03	459.208.671-67	2006-01-18	Lago de Gonçalves, 6 Piratininga 53005555 Moreira Grande / SP	92215-516
731	Felipe	Gomes	gcostela@example.net	PF	2017-07-21 08:57:00-03	971.548.302-04	1995-12-03	Esplanada Nascimento, 27 Beira Linha 18256-269 Souza do Oeste / MS	85811670
964	Bianca	Gomes	da-rosamariana@example.com	PF	2021-06-24 11:15:00-03	465.891.370-48	1970-08-10	Núcleo Lima, 184 Garças 64685-353 Gonçalves das Pedras / SP	71253-234
972	Daniel	Gomes	joao-felipe48@example.net	PF	2010-08-10 12:41:00-03	975.286.143-19	1991-10-24	Lago de Sales, 131 Itatiaia 06318-414 Silveira / AC	42328-349
73	Milena	Jesus	xvieira@example.com	PF	2012-06-21 11:25:00-03	925.037.416-06	1988-09-12	Parque Araújo, 352 Estoril 11290-320 Pinto da Serra / MA	74802-721
114	Rodrigo	Jesus	danielapinto@example.com	PF	2022-11-11 08:04:00-03	658.910.274-02	1949-03-21	Esplanada de Lopes, 86 Santa Monica 48561977 da Cunha / RS	84406630
298	Nicolas	Jesus	ryansantos@example.org	PF	2012-04-21 12:53:00-03	509.487.213-32	2001-01-19	Rodovia Ferreira, 21 Pindura Saia 42451-455 Barros da Serra / CE	04065-364
327	Maria Alice	Jesus	da-motaluiz-henrique@example.org	PF	2021-10-08 08:35:00-03	543.680.712-71	1951-02-26	Pátio Maria Clara Cavalcanti Beira Linha 75846700 Duarte / MS	32770-840
328	Benjamin	Jesus	davi-luccapinto@example.net	PF	2017-03-04 10:03:00-03	659.172.048-02	1971-10-11	Estação da Cunha, 64 Vila Santo Antônio Barroquinha 42157-327 Teixeira de Farias / RS	10559-302
523	Gabrielly	Jesus	laurada-mata@example.net	PF	2013-03-18 07:44:00-03	490.281.576-11	1951-01-05	Sítio de Moraes, 68 Nova Granada 53754-205 da Mata de Alves / RO	87182675
675	Camila	Jesus	barbosamaite@example.com	PF	2019-04-24 10:55:00-03	625.481.709-85	1963-08-25	Esplanada Fernando da Costa, 7 Vila Santo Antônio 24496828 Freitas / PA	96168071
696	Luiza	Jesus	costarenan@example.com	PF	2021-12-24 10:02:00-03	471.685.903-75	1996-06-16	Favela Ribeiro, 78 Delta 25034-385 Oliveira / AP	23037-823
822	Bárbara	Jesus	milenada-rocha@example.com	PF	2022-12-16 12:58:00-03	602.148.935-70	1952-07-29	Recanto Enzo Gabriel da Luz Vila São Francisco 39509-701 Melo / AM	76706-799
982	Beatriz	Jesus	fariasluiza@example.com	PF	2019-11-25 08:04:00-03	350.467.981-66	1949-09-08	Vereda de Alves, 96 Palmeiras 95190-216 Freitas / PR	26839-985
62	Ana Vitória	Lopes	pdias@example.net	PF	2010-12-02 10:14:00-02	431.286.957-09	1957-02-18	Campo João Felipe Nascimento, 2 São Bernardo 72648321 Pires Alegre / MG	56682892
152	Davi	Lopes	gcosta@example.com	PF	2017-07-01 11:56:00-03	104.258.973-97	2007-02-26	Quadra Pereira, 23 Cardoso 27987-882 Castro / AC	23590-188
291	Carlos Eduardo	Lopes	otavio24@example.org	PF	2012-02-26 09:34:00-03	852.193.064-05	1991-07-06	Alameda Isabel Silva, 31 Vila Havaí 11364-247 Nunes / DF	68301-753
350	Isis	Lopes	diogoaraujo@example.com	PF	2018-11-15 13:42:00-02	240.968.375-47	1961-01-08	Lagoa Benjamin Correia, 736 Suzana 65158388 da Costa das Flores / PR	07473442
427	Gabriel	Lopes	pazevedo@example.com	PF	2019-02-18 11:37:00-03	263.140.789-03	1955-12-09	Lago Maria Julia Rodrigues Taquaril 53270892 Lopes Verde / CE	44978329
439	Isabella	Lopes	igorcampos@example.org	PF	2019-05-18 08:42:00-03	570.462.381-62	1966-10-23	Rodovia de Novaes, 89 Boa Esperança 76062-783 Costa / AM	30513678
450	Thales	Lopes	uda-mata@example.com	PF	2019-04-01 09:40:00-03	503.681.924-15	1964-02-24	Esplanada Sales, 2 Califórnia 01064869 Melo / SE	88390892
573	Cecília	Lopes	joao19@example.com	PF	2021-05-11 12:05:00-03	149.035.782-32	1991-11-18	Campo Lucas Costa, 309 Universitário 27503-697 Cardoso de da Rosa / AM	55776517
604	Gabriela	Lopes	aliciasilva@example.com	PF	2022-03-20 13:32:00-03	761.045.329-34	1986-09-01	Vereda Oliveira, 56 Santa Rita De Cássia 72625-217 Pinto / SC	44743727
605	Sophia	Lopes	gabrielcampos@example.net	PF	2022-05-02 13:57:00-03	518.402.937-05	1986-12-09	Alameda Almeida, 8 Santa Isabel 62289-773 Freitas dos Dourados / RO	80918276
670	Marina	Lopes	goncalvesclara@example.com	PF	2022-10-25 11:11:00-03	926.153.874-64	1979-03-17	Aeroporto Fogaça, 6 Outro 84244907 das Neves / PR	21092961
825	Gustavo Henrique	Lopes	beatriz24@example.net	PF	2022-04-28 11:01:00-03	953.672.148-19	1990-03-11	Rua de Alves, 21 Gameleira 20078768 Viana Paulista / CE	84143888
864	Benício	Lopes	leandrodias@example.org	PF	2017-04-28 12:30:00-03	845.971.236-28	1998-07-03	Feira de Monteiro, 67 Vila Mangueiras 92992-146 Santos da Praia / SP	29563-668
99	Heloísa	Moura	manuela79@example.net	PF	2017-10-14 09:58:00-03	032.516.987-02	1945-02-18	Vale de Pinto, 77 Alpes 83294-846 Ramos / RJ	64841594
388	Heloísa	Moura	novaesluiz-fernando@example.org	PF	2017-02-14 10:43:00-02	340.875.619-84	1944-05-14	Praça Yuri Martins, 52 Itapoa 48374113 Azevedo / AM	06447-430
431	João	Moura	da-matabreno@example.org	PF	2013-11-20 09:12:00-02	415.702.836-80	1994-07-06	Viela Giovanna Cardoso Castanheira 31235258 Ferreira / TO	20931894
501	Evelyn	Moura	heitorgoncalves@example.org	PF	2018-06-20 11:51:00-03	501.986.743-84	2005-05-04	Campo Maria Eduarda Peixoto, 536 Europa 43797481 da Mata do Galho / SP	29074-901
556	Melissa	Moura	da-luzlorena@example.org	PF	2020-06-08 11:51:00-03	950.863.724-29	1947-03-27	Distrito Gabriel Cavalcanti, 48 Ouro Preto 68992416 Monteiro do Norte / BA	42839110
575	Bruno	Moura	theo66@example.com	PF	2015-07-27 08:15:00-03	619.478.203-03	2001-10-26	Travessa Ferreira, 96 Jatobá 98474439 Ferreira / TO	39617502
653	Maria Clara	Moura	ryan75@example.com	PF	2016-06-16 11:20:00-03	792.361.480-69	1985-04-03	Praia de Barbosa Bela Vitoria 48748186 Pereira do Oeste / SC	43287847
706	Juan	Moura	maria-fernanda15@example.org	PF	2019-01-10 13:36:00-02	754.391.820-04	2000-04-24	Jardim Caroline Teixeira, 46 Goiania 56851205 da Mata / MA	91580-802
796	Benício	Moura	emillyfogaca@example.org	PF	2019-11-08 12:59:00-03	748.206.193-22	1947-04-26	Passarela Mendes, 34 Lajedo 43858130 Porto / RS	39582-953
946	Brenda	Moura	levi65@example.org	PF	2016-03-05 07:07:00-03	284.179.053-32	1977-02-19	Feira de da Cruz, 282 Esplanada 65178537 Alves do Norte / TO	48090-423
990	Igor	Moura	duartesabrina@example.net	PF	2020-01-23 13:28:00-03	372.954.608-29	1993-10-22	Lago Novaes, 24 Boa União 1ª Seção 74015-731 Cardoso de Moura / MS	92666276
57	Luna	Nunes	gustavo12@example.org	PF	2017-05-04 13:51:00-03	618.429.573-09	1969-09-02	Vale de Rezende, 953 Ápia 55803-917 Moura / PA	80881095
254	Ana Carolina	Nunes	mcostela@example.net	PF	2012-10-24 11:48:00-02	612.835.409-24	1952-12-22	Recanto de da Luz, 9 Santa Sofia 78694-248 Nogueira do Sul / MA	98430781
303	Emilly	Nunes	antoniocavalcanti@example.org	PF	2017-04-11 10:13:00-03	310.924.875-14	1982-11-03	Trevo Gabriela Porto, 67 Colégio Batista 35807-329 Castro da Prata / AL	14585816
457	Camila	Nunes	pietra44@example.com	PF	2017-04-14 08:13:00-03	391.540.862-05	1979-04-30	Estação Azevedo, 75 Vila Da Luz 40014970 Cardoso / CE	47534-613
515	Ana Laura	Nunes	martinsdiego@example.com	PF	2014-04-17 12:32:00-03	457.206.891-76	2000-07-10	Residencial Kamilly Lima, 49 São José 32763-452 Costela / AL	81853963
616	Lucca	Nunes	fernandateixeira@example.org	PF	2017-08-04 07:47:00-03	218.695.730-21	1969-07-18	Lagoa de Pereira, 76 Xangri-Lá 73656217 Martins dos Dourados / AP	06759-880
638	Ana Carolina	Nunes	maria-sophia30@example.org	PF	2018-07-05 08:41:00-03	372.598.104-32	1979-01-21	Via Santos, 76 Mantiqueira 45911-781 Campos do Galho / RS	93769-315
717	Renan	Nunes	gpeixoto@example.net	PF	2015-02-21 10:48:00-02	349.281.567-73	1973-09-19	Jardim de da Rocha Conjunto Capitão Eduardo 14991503 Castro / RR	06635-669
836	Emanuelly	Nunes	vitor-gabriel68@example.com	PF	2022-08-23 08:20:00-03	752.986.041-02	1949-12-01	Trecho de Barros, 13 Frei Leopoldo 70568448 da Costa / TO	30163-306
871	Lucca	Nunes	alice24@example.org	PF	2022-08-22 11:00:00-03	159.034.268-24	1984-08-19	Núcleo Guilherme Porto, 2 Varzea Da Palma 25918-822 Rodrigues / AP	05487-000
879	Anthony	Nunes	barbarajesus@example.org	PF	2013-07-12 07:44:00-03	291.463.850-70	1962-08-03	Praça de Alves Santo André 10331-058 Costa / DF	15103-868
917	Theo	Nunes	diogofernandes@example.net	PF	2016-05-18 08:07:00-03	702.418.965-76	1947-12-13	Distrito de da Luz, 88 Cabana Do Pai Tomás 27391181 da Rosa / MS	15799-330
42	Alexandre	Pinto	vitor-hugobarbosa@example.net	PF	2013-04-12 09:10:00-03	058.623.179-02	1959-06-09	Ladeira Araújo, 39 Minaslandia 95368818 Ribeiro / AM	72785-264
117	Igor	Pinto	noahteixeira@example.net	PF	2022-06-22 07:15:00-03	039.452.618-05	1961-04-18	Campo Pires, 311 Conjunto Jatoba 63845-548 Silveira / SE	61967117
261	Maria Luiza	Pinto	maria-luiza75@example.net	PF	2015-09-20 09:35:00-03	580.469.173-20	1953-12-30	Avenida Nunes, 47 Vila Ipiranga 66706366 Rodrigues / ES	11151018
266	Nicolas	Pinto	danielribeiro@example.net	PF	2016-09-15 08:01:00-03	472.185.936-82	1994-10-06	Passarela de da Costa, 11 Betânia 12740926 Cardoso / TO	49973-191
292	Agatha	Pinto	carvalhovinicius@example.net	PF	2015-10-10 09:03:00-03	835.916.047-39	1985-04-23	Feira Elisa da Costa, 716  Jonas Veiga 78834-553 Moraes / RN	14310685
383	Calebe	Pinto	danielada-paz@example.org	PF	2017-01-24 11:00:00-02	930.854.627-83	1956-01-09	Condomínio de Moura, 56 São Gonçalo 00106888 Gonçalves / RS	93285-302
396	Maria	Pinto	luiz-felipeduarte@example.com	PF	2021-09-24 08:11:00-03	172.863.049-50	1972-09-03	Rodovia Sales, 1 Solimoes 57858-848 Lopes / PI	39842-347
580	Cecília	Pinto	rafaelnunes@example.net	PF	2014-05-04 10:13:00-03	502.837.146-62	1946-05-11	Travessa de Dias Guarani 97610874 Rocha / ES	35366-296
719	Daniel	Pinto	gabrielcunha@example.com	PF	2016-08-16 10:16:00-03	837.162.059-40	2001-05-23	Chácara de Ribeiro, 38 Funcionários 23426864 da Cruz de Melo / RJ	65540-758
805	Lara	Pinto	brunanunes@example.net	PF	2011-05-08 11:35:00-03	231.608.745-08	1953-06-30	Loteamento Augusto Cardoso Bom Jesus 80633-996 Rezende do Campo / SE	37512880
966	Carlos Eduardo	Pinto	baraujo@example.net	PF	2013-11-05 13:33:00-02	813.064.752-44	1943-12-23	Quadra Gabrielly Moreira, 2 Vila Maria 33753217 Gomes de Minas / SE	78125-333
52	Otávio	Pires	brunagoncalves@example.net	PF	2022-08-14 12:20:00-03	568.437.290-29	1955-02-26	Rodovia Isaac Silva, 99 Vila Santa Monica 1ª Seção 54647-803 Ramos de Minas / AC	91403292
82	Kamilly	Pires	joao-felipe15@example.org	PF	2019-07-03 10:17:00-03	518.329.470-41	1955-03-09	Esplanada da Cruz, 23 Vila Nova Cachoeirinha 3ª Seção 10616-772 Caldeira / AM	16914199
201	Olivia	Pires	samuelcardoso@example.org	PF	2021-11-12 13:43:00-03	530.247.619-34	1948-01-29	Ladeira Nicolas Cardoso, 319 Túnel De Ibirité 18390285 Cardoso / SE	92524874
250	Sophia	Pires	araujopietra@example.com	PF	2018-12-08 11:43:00-02	318.462.075-17	1964-07-03	Loteamento Bernardo Pinto, 7 Vila Pilar 00343041 Pereira / RN	48576950
269	Pedro Miguel	Pires	vitoria03@example.net	PF	2013-01-05 11:51:00-02	548.673.192-28	2002-06-18	Parque de Melo, 41 Piratininga 03368361 Peixoto / MG	29962-922
334	Marina	Pires	marina08@example.com	PF	2016-10-08 08:12:00-03	708.269.541-11	1998-06-28	Largo Yasmin Nascimento, 62 Fazendinha 98403464 Melo / ES	17185-932
433	Cecília	Pires	enzo-gabrielsouza@example.org	PF	2021-12-16 08:19:00-03	389.561.740-75	1980-02-07	Alameda Barros, 6 Vila Paquetá 49935-894 da Paz da Mata / AC	22473392
504	Isabelly	Pires	andre77@example.org	PF	2015-04-22 11:18:00-03	972.348.605-92	1953-12-23	Chácara de Caldeira, 6 Lajedo 03973-995 Pires Verde / MG	75910470
614	André	Pires	sophiaduarte@example.net	PF	2014-04-16 10:23:00-03	413.296.058-70	1989-06-05	Estação Pietra Costa, 19 São João 69750-619 Gomes / MG	94642477
645	Clara	Pires	estherdias@example.org	PF	2022-05-24 10:33:00-03	436.072.519-16	1963-04-10	Avenida Luiz Miguel Barros, 372 Miramar 49934530 Souza / RO	55592-946
672	Luiz Fernando	Pires	pedro-miguel33@example.org	PF	2019-07-14 09:16:00-03	679.408.123-78	2005-02-14	Estrada Gabrielly Ribeiro, 5 Alto Dos Pinheiros 82111-121 Cavalcanti das Pedras / MG	72526-425
973	Carlos Eduardo	Pires	emanuella37@example.net	PF	2020-12-25 12:51:00-03	745.983.062-38	1944-11-25	Praia Emilly Fernandes, 1 Vila Nova Gameleira 3ª Seção 43249646 Pinto dos Dourados / CE	99049657
67	Gustavo Henrique	Porto	monteiroraul@example.com	PF	2018-05-14 10:27:00-03	690.251.478-01	1959-03-05	Aeroporto Sarah Fernandes, 99 São Salvador 59863417 Duarte Paulista / SP	15270-670
422	Nina	Porto	eloahjesus@example.com	PF	2022-11-27 10:46:00-03	265.904.387-65	1961-02-28	Praça Gonçalves, 53 São Gabriel 77010127 Alves Verde / DF	10730-212
426	Ana Lívia	Porto	da-costaluiz-henrique@example.net	PF	2022-06-27 08:26:00-03	392.480.761-22	1975-04-27	Lagoa Calebe Fogaça Taquaril 68451073 Pires da Prata / MS	76188259
510	Bruno	Porto	calebe47@example.com	PF	2012-03-16 10:03:00-03	801.324.576-44	1978-06-14	Vereda Kamilly Duarte, 27 Flavio Marques Lisboa 06545-234 da Cruz do Sul / GO	68650457
627	Yasmin	Porto	theocavalcanti@example.com	PF	2020-12-14 10:51:00-03	423.867.950-47	1952-12-24	Esplanada de Lima, 77 Conjunto Califórnia Ii 30064-357 da Costa dos Dourados / PI	97109620
771	Ana Vitória	Porto	evelynmoura@example.org	PF	2020-01-16 09:55:00-03	206.841.539-98	1991-03-28	Trevo Correia, 78 Vila Santa Rosa 07185785 Carvalho de Rocha / GO	15071262
987	Clara	Porto	moraesryan@example.org	PF	2014-08-02 11:57:00-03	140.389.652-60	1955-06-20	Viaduto Duarte, 76 Manacas 99172-961 Gomes da Serra / MA	23460-398
32	Heloísa	Ramos	bda-mata@example.com	PF	2017-12-14 12:59:00-02	361.205.479-16	1954-05-11	Via de Alves, 97 Vila Piratininga Venda Nova 71086-408 Cardoso / MT	17028-400
191	Nicole	Ramos	csantos@example.org	PF	2013-03-23 11:14:00-03	371.605.498-48	1987-06-17	Favela Ribeiro, 419 Vila Suzana Segunda Seção 40372-350 Porto / MT	00802-813
232	Lorena	Ramos	livia43@example.net	PF	2013-11-27 14:57:00-02	851.673.209-68	1973-03-12	Recanto da Conceição, 7 Itatiaia 08586-084 Alves Verde / BA	24779-740
379	Enrico	Ramos	nda-rocha@example.com	PF	2021-02-21 10:18:00-03	502.678.493-38	1974-03-12	Lago de Barros, 894 Marieta 1ª Seção 77018201 Santos / DF	50913156
418	Stephany	Ramos	felipegomes@example.org	PF	2016-05-12 09:48:00-03	671.043.589-00	1981-08-01	Viela Nogueira, 14 Caetano Furquim 18440-482 Mendes da Serra / AC	25306594
446	João Gabriel	Ramos	leandro65@example.com	PF	2015-10-08 10:35:00-03	438.527.906-38	1997-04-09	Núcleo Davi Lucca Fernandes, 142 Vila São Gabriel 85521997 Rezende / SP	69463-503
484	Lorena	Ramos	ana-sophiaaraujo@example.com	PF	2021-07-01 09:51:00-03	291.547.063-43	2005-12-31	Setor de Correia, 76 Novo Glória 35723978 Pinto / PI	35663647
490	Agatha	Ramos	sarahgoncalves@example.org	PF	2021-06-07 07:30:00-03	582.437.190-32	1944-10-15	Esplanada Valentina Costela, 6 Antonio Ribeiro De Abreu 1ª Seção 06588-008 Caldeira do Sul / GO	18237366
496	Ian	Ramos	benicio20@example.com	PF	2020-02-27 09:50:00-03	074.953.128-23	1988-11-10	Recanto de Cavalcanti, 9 Nossa Senhora Da Conceição 28708-559 da Mata / AC	05583827
507	Carlos Eduardo	Ramos	yago88@example.org	PF	2015-04-20 08:35:00-03	953.280.641-51	1975-11-17	Passarela Alana Nogueira, 631 Monte São José 81353488 Cavalcanti dos Dourados / MA	50196064
521	Bernardo	Ramos	joao-miguel16@example.net	PF	2022-09-13 10:05:00-03	872.360.951-68	1987-09-16	Via Mariane Nogueira, 33 Vila Ecológica 71729-790 Silveira do Norte / MS	29201854
611	Ana Julia	Ramos	pedro-miguelmartins@example.net	PF	2019-08-06 11:42:00-03	164.598.302-15	1967-04-02	Área de Pereira, 4 Caiçaras 68043-152 Barbosa da Mata / SP	62394839
623	Nina	Ramos	luiz-fernando54@example.net	PF	2018-06-12 08:47:00-03	126.435.980-24	1966-07-18	Alameda João Vitor Gomes, 93 São Benedito 55600759 Peixoto / MG	97022-268
705	Marcos Vinicius	Ramos	isabel99@example.org	PF	2015-07-24 10:19:00-03	245.963.078-65	1975-08-21	Estação Olivia Moreira, 2 Vila Real 1ª Seção 59820159 Sales Grande / MA	44030-253
804	Bruno	Ramos	natalia23@example.org	PF	2011-12-06 11:00:00-02	739.285.406-29	1975-11-06	Passarela da Rocha Cruzeiro 90361423 Barros / MS	23047749
815	Marcos Vinicius	Ramos	samuel86@example.com	PF	2021-06-21 11:04:00-03	985.320.614-51	1982-04-21	Trecho de Dias, 6 Marçola 40811068 Caldeira / RJ	59134843
859	Sophie	Ramos	dda-cruz@example.org	PF	2020-11-03 08:41:00-03	629.438.570-92	2001-04-02	Campo Mendes, 3 Juliana 18036-669 Dias dos Dourados / MS	03168134
989	Carlos Eduardo	Ramos	thiago39@example.net	PF	2021-11-25 11:47:00-03	365.197.480-48	1948-09-02	Colônia Cardoso, 4 Ribeiro De Abreu 03645860 da Cunha da Praia / MA	59192733
8	Enzo	Rocha	caiolima@example.org	PF	2021-08-13 08:07:00-03	146.725.308-17	1980-06-21	Loteamento Felipe Rocha, 8 Novo Glória 45229611 da Paz / SC	06016884
27	Anthony	Rocha	rafaela93@example.org	PF	2022-05-10 09:29:00-03	642.810.579-94	2006-05-21	Ladeira Pedro Henrique Gomes, 7 Vila São Paulo 48885-536 Almeida / SP	96570869
30	Gabriel	Rocha	silveirabryan@example.net	PF	2018-01-26 12:53:00-02	625.940.783-10	1997-08-26	Largo Mariana Fernandes, 88 Castelo 78209280 Cardoso do Oeste / RS	93782-527
113	Lucas Gabriel	Rocha	alana30@example.net	PF	2020-01-07 11:29:00-03	713.895.042-04	2001-10-07	Viela Caroline Peixoto, 49 São Sebastião 86279204 Rocha dos Dourados / RN	58314-883
316	Carolina	Rocha	eloah92@example.net	PF	2018-01-23 13:29:00-02	370.564.298-74	1999-08-25	Alameda Olivia Sales, 986 São Bernardo 01819-608 Araújo do Sul / ES	50479649
377	Clara	Rocha	moraesrebeca@example.com	PF	2016-12-15 13:58:00-02	209.576.183-95	1983-02-15	Travessa João Felipe Mendes Paulo Vi 56830645 Correia de Souza / MA	78584-453
397	Bárbara	Rocha	martinsfernanda@example.org	PF	2018-10-13 07:51:00-03	031.694.287-13	2006-11-20	Vale da Costa, 39 Vista Alegre 59154-504 Nunes de Fernandes / PA	49504138
441	Bryan	Rocha	salesluiz-felipe@example.net	PF	2019-06-05 13:33:00-03	945.826.703-00	1970-10-04	Sítio Mariane da Mota, 85 Gutierrez 24729-115 Freitas de Aragão / MT	36846-137
480	Gustavo Henrique	Rocha	liviabarbosa@example.com	PF	2012-12-09 09:58:00-02	085.971.624-49	1968-11-10	Alameda Oliveira, 42 Horto 57639-337 da Rosa / AM	51705023
483	Luiz Otávio	Rocha	caldeiramaria-fernanda@example.net	PF	2022-10-23 08:12:00-03	619.428.705-58	1946-12-09	Feira Monteiro, 47 Vila Nova 65931475 Castro / MA	29387-861
505	Vitor Gabriel	Rocha	brunodas-neves@example.com	PF	2013-07-27 10:47:00-03	439.862.105-98	1986-05-26	Condomínio de Ferreira, 4 Ermelinda 72207453 da Paz do Oeste / RO	15590730
802	Esther	Rocha	steixeira@example.org	PF	2015-11-05 09:08:00-02	670.251.398-40	2006-03-30	Jardim de Pinto, 52 Bonsucesso 60509-385 da Luz de Correia / BA	64912-312
818	Yago	Rocha	bruna38@example.com	PF	2021-05-08 09:31:00-03	654.789.301-48	1993-03-01	Praça de Castro, 10 Gameleira 57583291 Martins / CE	22445-315
66	Eduardo	Sales	eloahbarbosa@example.org	PF	2021-05-19 10:18:00-03	917.624.538-19	1953-05-13	Viela Silveira, 47 Alípio De Melo 29229921 Freitas / AM	97949-601
140	Cecília	Sales	moreiraana-carolina@example.net	PF	2015-02-10 11:47:00-02	062.451.389-06	2001-04-18	Vale Sales, 7 Varzea Da Palma 08535-205 Cardoso de Nunes / TO	98230-713
190	Juliana	Sales	jmoreira@example.org	PF	2014-10-17 11:48:00-03	063.891.472-87	1973-12-14	Avenida Elisa da Conceição, 69 Horto 93717-436 Barbosa das Pedras / AL	17350-278
193	Ana Sophia	Sales	freitasheloisa@example.net	PF	2014-06-24 11:39:00-03	324.870.516-71	1978-04-10	Via Lavínia Ferreira, 86 Parque São Pedro 66412-888 Aragão Alegre / CE	84050-003
345	Renan	Sales	joao-miguel93@example.net	PF	2017-12-08 11:24:00-02	684.097.531-20	1970-12-08	Estação Isis Costela Jardim América 48676310 Martins do Sul / TO	69493-799
544	Raquel	Sales	lorenzoazevedo@example.org	PF	2018-09-08 09:35:00-03	962.518.730-86	1971-10-29	Largo Yuri Melo Nova Cintra 53237-146 Araújo / MT	95017-932
593	Maria Eduarda	Sales	paulomoraes@example.org	PF	2019-01-12 08:56:00-02	430.278.965-47	1943-04-04	Morro Rezende, 72 Conjunto Capitão Eduardo 10739-282 das Neves / MT	26064-419
615	Vitória	Sales	dda-costa@example.com	PF	2021-06-11 13:02:00-03	567.094.218-30	1990-09-23	Distrito Davi Lucca Novaes, 72 Horto Florestal 73320-109 Alves de Sales / AM	13709-647
680	Enzo	Sales	nathanrezende@example.org	PF	2016-08-07 09:18:00-03	935.627.401-07	1953-03-11	Lagoa de Silveira, 52 Conjunto Jardim Filadélfia 51841-485 da Costa / GO	52737-030
758	João Miguel	Sales	brenda09@example.org	PF	2014-12-25 11:17:00-02	609.483.271-03	1956-11-15	Viaduto Castro, 25 Grota 32361186 Almeida / ES	68408-718
830	Paulo	Sales	emanuella07@example.net	PF	2018-01-11 12:07:00-02	472.935.086-38	2004-05-28	Alameda Stella Dias Pongelupe 02733-045 Cunha Alegre / MG	21494-484
843	Maria Fernanda	Sales	piresdaniela@example.net	PF	2019-08-09 07:35:00-03	930.648.725-83	1969-10-13	Chácara Azevedo Vila Satélite 19973200 Costela de Pires / RJ	03740382
873	Isabelly	Sales	duartelais@example.org	PF	2021-05-25 12:15:00-03	105.892.673-02	1976-08-17	Rua Kevin Santos, 39 Custodinha 73653949 Cavalcanti / BA	19148089
931	Stephany	Sales	xmelo@example.org	PF	2020-09-14 11:09:00-03	659.037.814-20	2000-11-04	Lagoa Cavalcanti Ouro Preto 47590033 Pinto / MG	62871467
952	Stephany	Sales	renan82@example.com	PF	2019-12-04 09:14:00-03	926.134.570-07	1950-02-06	Avenida Ferreira, 83 Braúnas 60747604 Rocha / MA	50636-754
89	Ana Beatriz	Silva	pteixeira@example.org	PF	2021-08-26 10:23:00-03	529.410.638-89	1956-01-26	Estrada de Araújo, 55 Maria Helena 78047-637 Silveira / TO	53954-224
97	Emanuel	Silva	lara31@example.net	PF	2021-09-28 11:36:00-03	817.463.290-50	1956-07-07	Jardim Eduarda Mendes, 69 Guaratã 72017454 Aragão de Mendes / SC	35556-306
100	Maria	Silva	ana-sophia71@example.net	PF	2017-03-10 12:18:00-03	516.720.498-49	1998-12-22	Travessa Almeida, 1 João Paulo Ii 18073-484 da Paz das Flores / RN	74509-994
171	Arthur	Silva	pedro-lucassilveira@example.net	PF	2022-03-04 13:24:00-03	945.132.768-19	1987-07-02	Fazenda Lívia Teixeira, 5 Bandeirantes 57287814 da Cruz / AM	58058-025
224	Daniel	Silva	nmoura@example.org	PF	2019-09-20 09:30:00-03	243.781.560-08	1984-10-28	Praça Luiz Fernando Viana, 834 Vila Bandeirantes 73051762 Azevedo de Azevedo / PB	39836037
225	Leandro	Silva	hmoraes@example.com	PF	2020-08-18 11:58:00-03	073.691.854-00	1987-08-08	Viaduto de Viana, 105 Jaqueline 91856879 Cardoso do Campo / PR	30186-893
258	Kamilly	Silva	nunesana-clara@example.org	PF	2011-08-21 11:49:00-03	251.763.948-55	1961-08-12	Alameda Antônio Carvalho, 54 Olaria 53364659 Caldeira dos Dourados / PB	53456544
279	Heloísa	Silva	nuneslarissa@example.com	PF	2018-04-09 08:15:00-03	409.567.832-10	1972-11-24	Estrada Vinicius Rezende, 35 Santa Branca 42094682 Silveira / MG	80355-665
330	Ana Clara	Silva	da-luzalice@example.com	PF	2022-06-11 12:44:00-03	950.681.274-85	1953-02-14	Rodovia Benjamin Araújo, 16 Vila Bandeirantes 53588419 da Conceição / MT	59204955
358	Natália	Silva	lavinianascimento@example.com	PF	2019-03-15 12:56:00-03	419.283.657-28	1974-03-28	Praça Gonçalves, 8 Lindéia 81429425 Porto / PB	14500-907
382	Luiza	Silva	araujomariane@example.com	PF	2010-02-02 12:28:00-02	236.854.107-17	1962-02-16	Quadra Duarte, 428 Bela Vitoria 06743246 Ferreira / MG	11019-585
456	Gustavo Henrique	Silva	isaacpeixoto@example.com	PF	2014-02-12 13:54:00-02	148.236.095-06	1994-03-01	Alameda Farias, 16 Novo Glória 82970783 Rocha do Campo / RN	73671994
465	Cauê	Silva	caldeirabernardo@example.net	PF	2017-10-07 11:54:00-03	325.816.074-08	1987-11-29	Pátio de Peixoto, 26 Santa Rita 29374-160 Santos / SC	77592-484
606	Catarina	Silva	monteiropedro@example.com	PF	2021-06-18 11:14:00-03	625.098.741-01	2007-02-19	Travessa Juan Cavalcanti, 35 São João 04630205 Nascimento / PB	68273-210
610	Raquel	Silva	ana-lauralima@example.net	PF	2020-11-01 11:28:00-03	258.639.704-56	1999-02-15	Conjunto Ana Laura Farias, 7 Jaqueline 05466-526 Aragão de da Mata / MG	78697337
756	Letícia	Silva	alanasales@example.com	PF	2021-01-16 09:06:00-03	185.430.697-93	1990-03-02	Trecho Moreira, 617 Ribeiro De Abreu 80132149 Sales / MS	97477173
849	Pietra	Silva	da-motaisabelly@example.com	PF	2021-06-05 08:43:00-03	276.319.548-28	1980-12-16	Campo de Barbosa, 50 Ernesto Nascimento 85362-692 Silveira / AM	20568868
882	Isadora	Silva	stephanylopes@example.org	PF	2020-09-20 09:10:00-03	108.639.542-51	1999-03-02	Largo Ramos, 414 Vila Cemig 42969-918 Farias / MT	14677-152
886	Lucca	Silva	barbaradias@example.org	PF	2014-04-26 12:43:00-03	249.703.615-25	1952-02-02	Viaduto Maria Vitória Gonçalves, 4 Urca 49189394 da Mata Paulista / MS	25788873
901	Bruno	Silva	acarvalho@example.org	PF	2020-01-18 10:48:00-03	027.815.934-60	1987-09-30	Ladeira Fogaça, 63 Minas Brasil 63151181 Teixeira / BA	96961992
940	Mariana	Silva	marina51@example.org	PF	2016-10-25 09:53:00-02	187.394.560-48	1953-05-30	Vila Gomes, 2 Dom Joaquim 23336341 Alves de Goiás / PB	04974394
985	Giovanna	Silva	nogueiragabriel@example.com	PF	2018-01-18 13:15:00-02	398.674.215-82	1942-08-03	Setor Diego Sales, 54 João Alfredo 66264185 Duarte / PI	94645786
6	Ana Lívia	Souza	calmeida@example.com	PF	2010-03-09 09:09:00-03	470.631.825-44	1943-04-24	Largo Alexandre da Rosa, 1 Novo Ouro Preto 31934421 Novaes de Cardoso / AC	71428-512
17	Erick	Souza	sarahda-cruz@example.org	PF	2020-03-12 09:51:00-03	492.861.057-11	1992-10-05	Núcleo Isabel Cunha, 21 Mineirão 97284-576 Rocha / PR	70742-709
259	Brenda	Souza	amendes@example.org	PF	2019-10-12 09:17:00-03	094.615.782-02	1975-05-26	Núcleo de Moraes, 87 Jardinópolis 06008-538 Oliveira / MG	64223293
354	Isadora	Souza	caldeiraemanuelly@example.net	PF	2020-02-24 12:41:00-03	715.834.290-50	2003-03-17	Vale Leandro Pires, 69 Jardim Do Vale 00139-816 Melo / AM	45708-495
374	Lorenzo	Souza	qsilveira@example.net	PF	2013-07-28 08:52:00-03	951.873.026-12	1956-07-10	Travessa Caldeira, 6 Boa União 1ª Seção 15639600 Monteiro / AC	80623-031
380	Francisco	Souza	ana-julia74@example.com	PF	2015-10-25 13:27:00-02	098.754.162-58	1996-08-15	Setor Ribeiro, 36 Dom Silverio 11812-186 Monteiro do Norte / PE	38887-523
429	Evelyn	Souza	da-cunhathiago@example.net	PF	2015-05-28 12:38:00-03	732.059.648-29	1972-08-13	Praça de Ferreira, 33 Padre Eustáquio 47737-733 Gomes de Goiás / MA	14782-172
497	Ana Lívia	Souza	larissa08@example.com	PF	2021-10-25 13:18:00-03	467.038.529-10	1946-11-14	Campo Melissa da Costa, 49 Outro 82415-948 Santos da Mata / RR	98411-088
571	Emilly	Souza	da-conceicaogiovanna@example.com	PF	2018-01-18 12:36:00-02	932.806.154-70	1992-09-27	Morro Ana Julia Lopes, 79 Conjunto Jatoba 81875740 Farias / CE	66019018
601	Laís	Souza	ana-julia25@example.com	PF	2019-06-24 07:12:00-03	592.804.617-02	1998-11-08	Vereda de Ferreira, 17 Diamante 90493973 Cavalcanti Paulista / MG	26538-781
692	Maria Vitória	Souza	xazevedo@example.com	PF	2014-07-11 09:48:00-03	897.216.530-12	1990-11-28	Morro de Alves, 55 Sport Club 69961-611 Nogueira / TO	74975416
767	Yuri	Souza	nicole73@example.com	PF	2019-12-06 12:52:00-03	653.217.804-71	1975-01-20	Loteamento Moraes, 74 Vila São Geraldo 98347-948 Castro do Campo / AC	46688811
868	Davi Lucas	Souza	limabarbara@example.net	PF	2022-12-02 10:06:00-03	473.862.059-29	1973-12-21	Travessa de Pinto, 90 Estrela Do Oriente 16648-767 Rodrigues / PR	21603-931
898	Enzo Gabriel	Souza	heloisa15@example.net	PF	2013-02-25 12:54:00-03	325.168.074-90	1957-11-29	Aeroporto Rocha, 61 Vila Cloris 99323678 Moraes Alegre / PR	00147-561
965	Lorena	Souza	francisco43@example.net	PF	2016-08-28 11:28:00-03	210.574.386-26	1947-09-24	Pátio Ana Luiza Moraes, 39 Confisco 00785025 Teixeira / BA	49204-295
142	Luiz Otávio	Viana	noahda-costa@example.net	PF	2021-10-04 12:56:00-03	643.098.127-40	2005-06-17	Vale Melo, 96 Vila Maloca 87344059 Fernandes de Goiás / RR	58446-711
371	Júlia	Viana	maria-alice34@example.org	PF	2012-12-04 10:48:00-02	203.167.548-62	1949-01-07	Trecho de Barbosa, 78 Jardim Leblon 96711910 Porto / ES	73121-676
425	Valentina	Viana	vitornovaes@example.com	PF	2021-01-22 10:13:00-03	015.926.347-61	1969-06-16	Parque Gomes, 546 Nova Cachoeirinha 28585-458 Moraes de Goiás / SP	01609847
445	Mariane	Viana	vinicius22@example.net	PF	2019-10-18 11:44:00-03	576.218.940-67	1996-12-16	Distrito de Correia, 56 Mantiqueira 10907-694 Fogaça / MS	73044-289
530	Lorena	Viana	catarina63@example.net	PF	2018-02-14 10:30:00-02	163.208.794-40	1949-02-25	Rua Raul Rezende, 47 Conjunto Taquaril 87644113 Ferreira da Praia / PI	40085637
612	Clarice	Viana	bcorreia@example.net	PF	2017-02-05 10:35:00-02	812.340.597-97	1963-01-08	Avenida de Gomes, 5 Coração De Jesus 66123471 da Conceição / PR	79919593
654	Elisa	Viana	gramos@example.org	PF	2019-05-24 09:34:00-03	864.952.713-28	1973-01-31	Lago Daniel Azevedo, 400 Vila Independencia 3ª Seção 79178213 da Mota / BA	80530791
698	Heitor	Viana	lpires@example.com	PF	2018-08-14 12:16:00-03	768.245.390-74	1943-06-10	Viaduto Jesus, 80 Itaipu 72138-616 da Conceição das Flores / MS	86122-560
733	Gabrielly	Viana	da-motaeloah@example.com	PF	2020-09-26 07:37:00-03	540.163.289-42	1978-05-29	Avenida Júlia Martins Beira Linha 70330239 Jesus do Galho / ES	97449-955
833	Rebeca	Viana	piresmarcela@example.com	PF	2014-07-17 09:29:00-03	832.054.796-29	1973-09-22	Chácara de Caldeira, 93 Vila Jardim Alvorada 53433330 Moura / AL	81548-188
857	João Gabriel	Viana	rezendemaria-cecilia@example.org	PF	2017-05-14 11:29:00-03	925.036.817-86	1982-10-21	Largo Gustavo Campos, 54 Novo São Lucas 89590255 Barros de Vieira / RJ	93994345
896	Marina	Viana	aragaoagatha@example.org	PF	2018-06-19 11:13:00-03	246.385.901-60	1999-08-06	Chácara Cunha Madre Gertrudes 93492613 Porto / CE	31235095
906	Emanuella	Viana	alicialopes@example.net	PF	2012-11-25 12:07:00-02	430.851.762-17	1990-08-04	Conjunto de Moura, 608 Vila Antena 19804554 Azevedo / RR	48641-854
922	Anthony	Viana	maria-ceciliacorreia@example.com	PF	2022-08-17 08:05:00-03	576.049.832-00	1965-09-27	Núcleo Evelyn Porto, 68 Minas Brasil 40353-580 Santos Alegre / RR	22237-337
928	Ana Lívia	Viana	vitor58@example.com	PF	2022-12-16 07:46:00-03	785.431.629-55	1975-01-08	Chácara de Sales, 6 Novo Tupi 64262-820 Lopes de Costa / GO	42445519
936	Luiz Fernando	Viana	onovaes@example.net	PF	2017-03-27 13:43:00-03	310.657.829-77	1954-09-12	Rua Nascimento, 454 Barro Preto 07006068 Pereira do Sul / AP	63790069
19	Juan	Barros	oazevedo@example.net	PF	2019-10-05 10:24:00-03	250.861.493-98	1983-12-27	Lago de Moraes, 17 Vila Calafate 97747-354 Barbosa / AL	50402163
286	Arthur	Barros	aragaodiogo@example.org	PF	2021-07-12 09:27:00-03	042.981.536-06	1996-10-30	Quadra Luiz Felipe Silva, 66 Providencia 39170341 Monteiro do Galho / MG	95198-890
359	Renan	Barros	calves@example.org	PF	2019-01-15 09:27:00-02	145.028.679-85	1958-08-10	Rua Gabrielly Teixeira, 5 Vila Mantiqueira 57819735 Peixoto / SC	41597-578
363	Maria Luiza	Barros	mariana09@example.com	PF	2022-05-03 08:19:00-03	973.526.840-00	1991-10-12	Loteamento da Rocha Barão Homem De Melo 1ª Seção 67010422 Cardoso do Campo / MT	11833-620
410	Mirella	Barros	andre65@example.net	PF	2019-04-22 10:24:00-03	312.054.796-43	1979-05-08	Chácara Sales, 37 Olaria 01703-287 Duarte de Nunes / SE	86348520
576	Arthur	Barros	ana-carolina66@example.net	PF	2015-12-26 11:07:00-02	371.604.985-93	1978-07-24	Núcleo de Gonçalves Vila Suzana Segunda Seção 99295-632 Correia Paulista / MS	53833679
584	Nina	Barros	ana-julia39@example.net	PF	2014-11-07 09:59:00-02	928.647.153-09	1946-08-04	Passarela Nina Nunes, 578 Jardim Atlântico 09085-697 Costa / GO	53337997
595	Ana Sophia	Barros	da-matamarcela@example.com	PF	2016-10-11 12:15:00-03	796.521.408-30	1949-10-05	Vale Barbosa, 4 Madre Gertrudes 10531-758 Cunha / RR	72402214
624	Maria Vitória	Barros	pedro41@example.org	PF	2015-08-03 13:55:00-03	140.863.572-08	1962-08-07	Núcleo Pires, 93 Nossa Senhora Do Rosário 62644406 Caldeira / MG	66313047
657	Guilherme	Barros	costelacaua@example.org	PF	2020-03-06 10:40:00-03	340.178.269-03	1998-07-19	Quadra da Paz, 49 Novo Das Industrias 20437973 Almeida / SP	54404420
678	Caio	Barros	caua25@example.net	PF	2016-06-06 09:17:00-03	589.167.243-09	1984-06-24	Área de da Rosa, 7 Floramar 45453-267 Moreira / AP	46910487
704	Davi	Barros	maria-juliacarvalho@example.com	PF	2021-10-11 11:10:00-03	924.068.531-60	1954-02-27	Loteamento da Costa, 10 Jonas Veiga 66521-003 Ribeiro de Ramos / CE	01888-571
779	Ana Clara	Barros	ssilva@example.org	PF	2014-04-15 08:46:00-03	591.846.203-15	1968-03-21	Trecho de Pereira, 3 Novo Tupi 95361484 Melo Grande / AP	95294945
794	Isis	Barros	brunonunes@example.com	PF	2019-11-14 08:15:00-03	517.689.342-82	1962-05-06	Viela da Luz, 664 Vila São Rafael 24907824 Nogueira / PE	26042844
45	Bianca	Campos	leonardocorreia@example.org	PF	2015-10-19 12:51:00-02	783.250.419-60	1948-04-07	Pátio de Viana, 30 São Gonçalo 46881-197 da Rocha / PA	41039-098
75	Davi Lucas	Campos	da-pazeduardo@example.com	PF	2011-06-12 10:50:00-03	197.534.820-60	1951-09-17	Rodovia Francisco Cardoso, 408 Cabana Do Pai Tomás 58589-864 Carvalho / BA	54585-000
135	Calebe	Campos	yda-mata@example.com	PF	2016-07-03 09:30:00-03	482.190.675-94	1973-08-16	Morro de Melo Novo Glória 31679-981 Jesus de Araújo / MT	18419-509
230	Kamilly	Campos	renanferreira@example.org	PF	2020-12-22 13:37:00-03	768.241.309-31	1985-08-26	Morro Martins, 71 Renascença 86342232 Dias / MA	08164-995
274	Júlia	Campos	sophia62@example.org	PF	2018-08-16 09:55:00-03	563.072.984-56	2005-06-12	Alameda Moraes, 49 Leticia 42294034 Gomes / TO	19166-875
524	Pietra	Campos	camposmelissa@example.net	PF	2019-11-02 08:32:00-03	847.102.593-05	1983-10-12	Condomínio Rafael Carvalho Vila Independencia 2ª Seção 59366911 Caldeira / CE	64726243
588	Maria Vitória	Campos	alicevieira@example.com	PF	2019-09-07 12:19:00-03	103.894.675-10	1962-10-29	Núcleo de Campos, 46 Vila São Dimas 87547446 da Mota do Amparo / AP	77606835
635	Vitor	Campos	ccostela@example.net	PF	2022-09-12 12:38:00-03	293.106.475-07	1980-09-26	Jardim Marcela Costa Silveira 07501623 Rocha de Lopes / PA	66905558
641	Thiago	Campos	calebegoncalves@example.org	PF	2019-12-07 10:32:00-03	128.506.739-86	2001-12-25	Favela Lorenzo Rodrigues Bom Jesus 99335361 da Costa de Jesus / TO	51064-127
880	Benjamin	Campos	kbarbosa@example.net	PF	2017-06-06 11:49:00-03	568.249.701-58	1979-06-21	Avenida Carolina Costa, 740 Vila Puc 00510-876 Rezende dos Dourados / MG	06819237
902	João Miguel	Campos	luna56@example.com	PF	2018-03-17 12:41:00-03	534.018.962-70	1946-05-20	Largo de Silva, 1 Vila Da Amizade 11206-205 Pires / AM	64098823
37	Alice	Castro	gustavoalmeida@example.net	PF	2022-07-03 13:07:00-03	813.456.097-01	1967-11-19	Aeroporto Correia, 67 Campo Alegre 59473-288 Silva / CE	28693764
39	Thiago	Castro	marcelacaldeira@example.net	PF	2022-04-25 12:35:00-03	983.067.425-83	2005-09-05	Loteamento Diogo Almeida, 83 Aparecida 43075-851 Lima / RJ	35623-437
105	Letícia	Castro	theofernandes@example.com	PF	2018-11-27 13:02:00-02	726.380.541-35	2000-07-14	Chácara Vinicius Moura, 35 Nova Cintra 32979693 Moreira / BA	76910649
185	João Guilherme	Castro	theosilveira@example.org	PF	2011-03-23 11:06:00-03	583.412.067-90	1946-01-11	Parque Letícia Nunes, 65 Vila Calafate 27185265 Pires / PA	57735990
219	Anthony	Castro	natalia39@example.net	PF	2022-02-17 07:52:00-03	369.570.184-66	1961-07-24	Travessa Ramos, 76 Funcionários 81165476 Monteiro das Flores / PR	30145-805
290	Pietro	Castro	luna07@example.com	PF	2016-02-19 10:30:00-02	824.503.697-83	1981-08-12	Vila de da Paz Conjunto Serra Verde 96440703 Rezende de Araújo / AP	85964298
621	Júlia	Castro	gomeslucas@example.com	PF	2015-03-11 08:29:00-03	785.269.430-65	1970-04-07	Aeroporto Gabriela Araújo, 521 Horto Florestal 94393029 Peixoto / AM	75465498
763	Luiz Henrique	Castro	dlopes@example.com	PF	2016-10-19 12:58:00-02	094.816.725-49	1946-05-12	Lagoa de Gonçalves, 4 Bairro Das Indústrias Ii 48251962 Duarte / PA	55834-279
148	Fernando	Duarte	samuelmendes@example.net	PF	2016-02-22 08:31:00-03	390.641.857-00	1969-03-01	Praia Viana, 44 Capitão Eduardo 69451-123 Teixeira de da Mata / AL	28720-323
205	Ana Carolina	Duarte	dteixeira@example.net	PF	2016-01-15 09:42:00-02	164.780.395-01	1948-03-13	Praça Costa Manacas 12548-182 Oliveira da Serra / RR	05433470
288	Emanuel	Duarte	lorena85@example.com	PF	2018-10-08 07:40:00-03	965.748.021-30	1951-08-25	Residencial Cunha, 31 Ipiranga 14800-710 Viana do Norte / SE	88407-318
294	Lorenzo	Duarte	mariada-mota@example.net	PF	2021-05-13 08:44:00-03	806.954.321-33	1958-05-04	Morro de Lopes, 49 São Sebastião 11028-858 Pinto de Rezende / RR	14947282
360	Juliana	Duarte	pintoluiz-miguel@example.com	PF	2021-08-15 08:39:00-03	643.702.859-92	1986-05-07	Pátio de Lopes, 81 Santa Isabel 70488-300 da Cruz da Mata / SC	82878539
395	Marcela	Duarte	eda-conceicao@example.com	PF	2022-12-01 12:27:00-03	912.308.647-50	1983-04-18	Campo Monteiro, 57 Floresta 48388-235 da Rosa / TO	18074779
475	Mariana	Duarte	fogacaleandro@example.org	PF	2020-05-04 10:45:00-03	723.549.860-92	1957-10-02	Quadra Ana Carolina da Cunha, 95 Santana Do Cafezal 19991-246 Ferreira / PB	51519-852
487	Catarina	Duarte	xcastro@example.com	PF	2019-07-08 12:16:00-03	160.372.854-62	1948-02-26	Estrada de Azevedo, 127 Pedreira Padro Lopes 71412295 Silveira de Ribeiro / RN	77823-250
495	Emanuelly	Duarte	fbarbosa@example.org	PF	2015-07-02 08:07:00-03	036.928.541-70	1962-03-02	Jardim de Alves, 64 Parque São José 56848686 Fernandes da Mata / BA	05554873
537	Valentina	Duarte	laurada-rosa@example.net	PF	2020-02-02 13:09:00-03	495.786.031-93	1957-10-26	Aeroporto de das Neves, 29 Piraja 37700621 Castro de Cunha / BA	97397-921
548	Lavínia	Duarte	pcastro@example.net	PF	2021-05-24 09:19:00-03	713.602.495-16	1982-05-20	Parque de Jesus São Luiz 10227-287 Freitas das Flores / ES	37638910
564	Miguel	Duarte	ifreitas@example.com	PF	2015-05-20 08:34:00-03	947.613.058-00	1996-08-16	Esplanada de da Paz, 6 Monte São José 27113-651 Freitas / RS	36964637
586	Vinicius	Duarte	kamilly03@example.org	PF	2021-04-18 08:35:00-03	539.820.641-98	1967-04-30	Vereda Maria Vitória Cardoso, 68 São João 17109354 Almeida / PB	98536400
619	Julia	Duarte	vitor19@example.org	PF	2020-04-02 11:43:00-03	962.378.405-83	1964-06-06	Chácara Brenda Caldeira, 98 Tirol 94227264 Novaes / AM	47471-169
732	Melissa	Duarte	davi77@example.net	PF	2018-07-26 11:07:00-03	860.519.243-70	1980-05-29	Favela de da Paz, 52 Vila Aeroporto Jaraguá 40100034 Vieira / MA	38555902
842	Maria Julia	Duarte	marianenascimento@example.com	PF	2020-10-02 07:21:00-03	670.125.834-44	1990-10-29	Rua Bianca da Conceição, 59 Conjunto Paulo Vi 14021619 Silveira do Galho / PE	13536663
893	Lucca	Duarte	nicolelopes@example.net	PF	2022-07-19 10:30:00-03	176.043.259-80	1947-11-20	Lagoa Beatriz Correia, 93 Madre Gertrudes 06474810 Costa / ES	04783273
916	Lívia	Duarte	psilveira@example.org	PF	2020-01-17 08:30:00-03	039.285.671-95	1987-07-08	Distrito Nicole Alves, 28 Satelite 94123-751 da Mota / CE	46057-007
935	Ana Sophia	Duarte	rodrigonunes@example.net	PF	2018-03-22 12:23:00-03	459.360.281-51	1961-11-13	Loteamento de Silva, 66 Marmiteiros 19982817 Freitas / PE	25315-391
974	Maria Julia	Duarte	vianamarina@example.org	PF	2014-07-18 12:15:00-03	908.352.461-24	1999-04-01	Parque Gomes, 90 Mirtes 13551-776 da Rocha / RJ	23947-364
992	Larissa	Duarte	pietroda-rocha@example.com	PF	2020-02-13 09:01:00-03	453.702.916-16	1981-08-11	Estação Monteiro Nossa Senhora Da Aparecida 84445381 da Paz do Sul / MG	53041258
550	Ana Clara	Farias	isilva@example.com	PF	2017-06-25 12:12:00-03	159.704.632-99	1981-05-08	Travessa de Moreira, 579 Monte Azul 04729432 da Luz da Serra / AM	35253-970
553	Lavínia	Farias	marcos-vinicius92@example.org	PF	2017-10-25 10:12:00-02	019.538.276-59	1977-08-16	Jardim Melo, 90 Conjunto Taquaril 87215453 Alves de Porto / AM	73186-842
640	Davi Lucas	Farias	tmendes@example.org	PF	2020-06-11 11:58:00-03	973.184.620-40	1953-09-28	Lagoa Duarte, 3 Granja De Freitas 99584-579 Lopes das Pedras / AM	54416006
819	Valentina	Farias	samuel91@example.com	PF	2022-08-02 08:24:00-03	263.810.974-69	1970-09-14	Jardim Kevin Nogueira, 529 Capitão Eduardo 02187736 da Luz / RS	36872-995
870	Arthur	Farias	dxxteixeira@example.net	PF	2017-12-20 09:12:00-02	671.590.238-03	1998-11-04	Praia de Pires, 90 Vale Do Jatoba 32283688 Azevedo / GO	43431-930
978	Breno	Farias	lopesvitor-hugo@example.com	PF	2022-03-07 11:38:00-03	269.357.041-70	1949-01-05	Setor Gustavo Henrique Dias, 89 Jardim Montanhês 59366-662 Aragão / SP	72750-274
33	Vitor Gabriel	Mendes	gabrielada-conceicao@example.org	PF	2016-11-10 11:52:00-02	235.087.649-74	1985-07-25	Feira Moraes, 72 Piraja 92690-290 da Luz Grande / AC	58000199
101	Enrico	Mendes	da-costavitor@example.com	PF	2018-11-05 12:17:00-02	543.918.762-64	1974-09-06	Estação de Vieira Barão Homem De Melo 1ª Seção 55506-524 Barbosa de Viana / PE	54951-507
103	Isis	Mendes	raul30@example.net	PF	2018-05-25 10:51:00-03	370.592.846-56	1990-12-31	Alameda Teixeira, 42 Sport Club 87627-420 Gonçalves Alegre / RS	36001-295
213	Gabriela	Mendes	vcardoso@example.net	PF	2019-08-18 08:33:00-03	712.960.358-59	1991-08-01	Rodovia Otávio Caldeira, 348 Primeiro De Maio 60349419 da Cruz de Freitas / AP	02265-501
295	Kamilly	Mendes	gabrielly68@example.com	PF	2017-07-13 10:27:00-03	789.613.420-96	1970-01-10	Vereda de Aragão, 33 Goiania 85239-665 Silva / PA	35498603
323	Rafael	Mendes	marcos-viniciusmonteiro@example.org	PF	2015-06-14 13:51:00-03	687.341.059-48	1987-03-19	Feira de Pereira, 956 Itapoa 15282013 Viana / RR	78151-537
335	Daniel	Mendes	freitasjoana@example.org	PF	2022-01-02 08:09:00-03	103.986.527-59	2002-11-19	Colônia de Peixoto Carlos Prates 71637-087 Jesus dos Dourados / PE	95977393
476	Thiago	Mendes	enogueira@example.org	PF	2018-05-26 12:11:00-03	387.045.196-39	1944-09-24	Fazenda Yuri Ramos Nova Suíça 18318-047 Almeida da Mata / MA	43688-770
633	Caio	Mendes	diegocavalcanti@example.net	PF	2011-05-08 10:51:00-03	032.479.165-80	1960-09-27	Passarela Benício da Rocha, 7 Santa Inês 34620470 da Mata dos Dourados / PA	08093154
650	Brenda	Mendes	bmonteiro@example.org	PF	2022-07-12 10:16:00-03	964.357.208-00	2000-11-20	Feira Carvalho, 9 Sport Club 55050-039 Araújo Verde / MA	81446657
36	Enzo Gabriel	Moraes	beniciopinto@example.net	PF	2012-02-17 13:58:00-02	718.634.902-13	1956-06-04	Rua Cunha, 55 Vila Nova Dos Milionarios 16425-858 Nascimento / SC	36567-846
40	Marcela	Moraes	vitor92@example.org	PF	2015-04-14 11:50:00-03	587.192.463-82	1966-10-02	Lago de Cavalcanti, 24 Parque São José 53257316 Rezende do Galho / PB	68473580
228	Juan	Moraes	tcostela@example.net	PF	2022-03-04 10:41:00-03	456.932.780-00	1970-02-05	Quadra de Peixoto, 22 Vila Puc 62498734 da Rocha de Goiás / PE	30944-978
322	Enrico	Moraes	esthercaldeira@example.com	PF	2014-06-27 12:40:00-03	523.904.786-38	2006-08-20	Travessa de Fogaça, 90 Pousada Santo Antonio 09628-610 Cardoso / RO	29854724
412	Enzo	Moraes	marcelasales@example.net	PF	2020-12-01 11:13:00-03	193.520.874-88	1973-06-10	Área de da Rosa Santa Cruz 83177213 Gonçalves de Mendes / RR	19613-119
414	Sarah	Moraes	bianca37@example.org	PF	2017-04-10 11:29:00-03	854.176.290-49	2007-02-09	Campo Alícia Moura, 41 Barão Homem De Melo 3ª Seção 90575391 Pereira do Sul / PB	79765622
554	Emanuel	Moraes	santosenzo-gabriel@example.org	PF	2022-06-02 12:03:00-03	578.613.240-44	1943-10-02	Via da Paz, 9 Vila Da Ária 01280-233 Moura Alegre / AL	59958-346
683	Luiz Otávio	Moraes	araujoana@example.com	PF	2022-03-06 10:25:00-03	497.250.316-52	1962-08-07	Alameda de Pinto Paraíso 02797266 Sales Grande / AP	24508-434
708	Eduardo	Moraes	haraujo@example.net	PF	2020-07-19 08:21:00-03	653.879.041-01	1990-01-06	Favela Daniela Pereira, 320 Providencia 84467054 Sales do Norte / AM	16577479
738	João Vitor	Moraes	laisdas-neves@example.net	PF	2018-12-19 14:10:00-02	810.927.654-76	1961-08-17	Jardim Pietro Gonçalves Vila Real 1ª Seção 87016-537 Cunha de Rocha / PB	10322-871
778	Igor	Moraes	pietrosouza@example.net	PF	2018-12-21 12:46:00-02	471.206.598-20	1982-06-06	Vereda Vieira, 72 Carmo 07354490 Nascimento Paulista / RR	86050-358
800	Fernanda	Moraes	pietro80@example.com	PF	2015-02-25 12:05:00-03	730.695.284-65	1992-04-10	Conjunto Maria Sophia Porto, 92 Conjunto Santa Maria 76612-828 Ribeiro / RO	79107382
846	Júlia	Moraes	correiabryan@example.net	PF	2013-10-12 07:33:00-03	583.964.721-73	1989-12-30	Esplanada de Araújo, 89 Beira Linha 68431861 Campos / AL	08907828
877	Emanuella	Moraes	laura76@example.org	PF	2013-02-11 09:31:00-02	149.572.036-52	1944-05-28	Distrito Duarte, 27 Vila Betânia 12196-782 Barbosa de Nunes / AM	48120724
912	Bruna	Moraes	carolinepeixoto@example.net	PF	2020-07-16 09:22:00-03	043.586.179-48	1991-10-17	Praia Heitor Martins, 550 Vila Atila De Paiva 54339-769 da Mata / PA	02217-338
918	Julia	Moraes	laisnogueira@example.com	PF	2020-05-16 08:58:00-03	518.302.649-15	2003-07-15	Setor Fogaça Mirante 97926-776 da Paz do Galho / MS	27479-650
938	Isabel	Moraes	ana-luizacampos@example.com	PF	2016-12-12 10:27:00-02	542.783.619-55	1998-01-05	Fazenda João Lucas Lima Nossa Senhora Aparecida 67019105 das Neves do Oeste / SC	26391863
983	Manuela	Moraes	agomes@example.net	PF	2019-04-15 09:52:00-03	415.369.827-09	1974-02-14	Estrada Emanuella Rodrigues, 71 Vila Antena Montanhês 80401-864 Duarte / PR	18035963
20	Lara	Novaes	ana-vitoriada-mota@example.com	PF	2021-05-20 12:06:00-03	195.243.680-05	1977-10-30	Chácara Kaique Souza, 43 Granja De Freitas 48647-530 Vieira de Minas / AC	77067-771
29	João	Novaes	xpinto@example.net	PF	2015-07-27 08:08:00-03	607.241.958-58	1969-05-25	Área Cardoso, 155 Betânia 18705056 Freitas / PA	92648022
51	Pietra	Novaes	afreitas@example.com	PF	2022-01-18 11:52:00-03	641.978.520-02	2000-09-10	Rodovia Campos, 83 Vila Nova Cachoeirinha 1ª Seção 98409255 Jesus do Galho / RS	20739-657
120	Lucas Gabriel	Novaes	opereira@example.org	PF	2021-11-18 07:05:00-03	948.507.132-97	1968-04-26	Vila de Monteiro, 8 Marieta 2ª Seção 99015734 Castro do Sul / AP	37378-687
168	Bárbara	Novaes	lucas-gabrielnovaes@example.org	PF	2012-12-20 14:11:00-02	709.186.435-20	1948-07-15	Passarela de Novaes, 10 Boa União 2ª Seção 79113654 Santos / RJ	88515-404
520	André	Novaes	da-motahenrique@example.net	PF	2019-11-03 13:26:00-03	954.261.073-43	2000-04-03	Colônia de Dias, 249 Coqueiros 75446-716 da Luz de Correia / RJ	04127-343
543	Ana Júlia	Novaes	antonio35@example.net	PF	2017-07-05 12:58:00-03	024.719.658-49	2003-11-08	Viaduto Barbosa, 58 Etelvina Carneiro 15413-609 Fernandes / TO	95011-680
598	Davi Luiz	Novaes	andregomes@example.com	PF	2015-02-09 11:15:00-02	012.473.695-52	1999-07-01	Colônia Ana Novaes, 97 Prado 92364159 Lima / MS	84829001
648	Renan	Novaes	fariasjuan@example.org	PF	2021-07-28 12:32:00-03	048.261.935-05	1989-03-05	Loteamento Cunha, 14 Serrano 79284-464 Teixeira da Prata / MA	81822-316
744	Clarice	Novaes	gustavo55@example.org	PF	2012-03-21 08:46:00-03	327.805.914-04	1942-07-28	Pátio de da Costa, 78 Túnel De Ibirité 91951-698 Barbosa / PE	65986-747
762	Sabrina	Novaes	ferreirarenan@example.org	PF	2022-09-15 11:36:00-03	862.174.053-26	1967-05-06	Parque Pires, 66 Solar Do Barreiro 16251606 Martins da Mata / DF	73222953
844	Ana Laura	Novaes	maite19@example.org	PF	2022-09-19 09:48:00-03	932.516.784-00	1955-05-20	Setor de Costa, 810 Colégio Batista 39328445 Oliveira de Minas / ES	44399366
981	Kamilly	Novaes	elisa65@example.net	PF	2021-09-13 11:38:00-03	784.526.130-08	1959-08-14	Travessa Porto, 1 Ouro Preto 93007811 Viana do Campo / RO	02101-190
13	Sophie	Santos	tsantos@example.org	PF	2022-12-16 08:20:00-03	691.038.475-00	1992-04-10	Praça Maria Luiza Peixoto, 8 Sion 41511-505 da Luz do Sul / TO	51923031
60	Luiz Fernando	Santos	maria-sophia76@example.com	PF	2017-11-28 12:52:00-02	601.827.534-17	1963-04-30	Núcleo Leonardo Carvalho, 2 Santa Sofia 98591930 Castro / TO	19629-983
151	Maitê	Santos	mcardoso@example.net	PF	2018-03-09 12:53:00-03	765.410.892-30	1989-05-20	Via Peixoto, 382 Vila Formosa 31903911 da Mota / ES	35686159
188	Bárbara	Santos	benjaminmoreira@example.org	PF	2022-11-12 11:02:00-03	496.325.108-60	1951-07-06	Sítio de Sales, 90 Pongelupe 09139183 Pereira / RJ	23499-744
204	Murilo	Santos	augustopeixoto@example.org	PF	2021-02-23 13:46:00-03	831.250.967-40	2002-03-06	Aeroporto Fogaça, 85 Flavio Marques Lisboa 45322737 da Costa / MT	02417-883
237	Diogo	Santos	cecilia66@example.com	PF	2021-07-28 08:45:00-03	630.819.245-70	1978-02-21	Vale de Cunha, 28 Coração Eucarístico 57431782 da Cruz / SE	50773-129
276	Ryan	Santos	julianafarias@example.com	PF	2021-02-03 08:25:00-03	861.397.542-91	1990-02-15	Estação de Oliveira Gutierrez 96932657 da Cunha / PR	15309-406
341	Maria Vitória	Santos	rezendejoao-gabriel@example.com	PF	2016-04-12 09:25:00-03	382.709.456-92	1988-02-17	Esplanada da Cunha, 93 Vila Atila De Paiva 68152259 Peixoto da Praia / AC	73042220
406	Emanuella	Santos	felipe23@example.com	PF	2017-08-25 10:16:00-03	961.807.423-40	1957-02-18	Sítio de Rezende, 8 Anchieta 43074-122 Cardoso / PR	44099-630
436	Amanda	Santos	ana-juliamendes@example.com	PF	2019-12-10 09:32:00-03	071.684.293-96	1972-04-01	Conjunto Pedro Miguel das Neves, 44 Boa Esperança 05815884 Pereira / RJ	17775-526
462	Paulo	Santos	emanuellaalves@example.com	PF	2016-11-19 11:53:00-02	452.986.130-98	1986-08-30	Colônia Vitória Jesus, 77 Castelo 11731280 Cardoso Verde / AM	72543224
502	Bruna	Santos	oalves@example.org	PF	2021-12-19 12:05:00-03	506.798.431-75	2001-07-13	Trecho Pereira, 2 São Cristóvão 02107640 Barros / SP	81864640
651	Maria	Santos	fernandesclarice@example.org	PF	2016-07-14 09:22:00-03	781.952.634-37	1990-02-08	Condomínio Maria Julia Santos, 808 Jardinópolis 75099253 Peixoto de Minas / CE	55688182
750	Vitória	Santos	pintojoao@example.net	PF	2012-05-05 08:32:00-03	865.014.973-10	1958-08-10	Vila de da Cunha, 34 Anchieta 28167329 Silva da Praia / PA	37929375
754	João Vitor	Santos	calebecampos@example.net	PF	2022-12-15 11:27:00-03	458.716.320-17	1969-01-03	Loteamento Nunes, 1 Madre Gertrudes 69306319 Aragão Alegre / GO	33631-389
810	Erick	Santos	costelajoao-guilherme@example.org	PF	2017-07-07 12:17:00-03	620.154.938-24	1990-05-05	Residencial Barros Pousada Santo Antonio 00818-311 Mendes de Barros / RO	55910-812
920	Clara	Santos	ian88@example.net	PF	2021-01-22 10:48:00-03	608.943.715-84	1963-02-19	Sítio Ana Júlia Novaes, 7 Jardim São José 85362-106 Moreira Paulista / SC	76057165
943	Lucas Gabriel	Santos	ana-clara77@example.org	PF	2017-09-07 12:09:00-03	375.698.102-95	1942-08-09	Rodovia de Cavalcanti, 2 Santa Maria 85253-940 da Mota / ES	37436528
970	Davi Luiz	Santos	pedro-miguel99@example.org	PF	2021-01-07 12:42:00-03	129.680.347-31	1965-02-05	Quadra de Barbosa, 38 Castanheira 15014-395 Fernandes das Flores / AL	02065-237
84	Lucas	Vieira	maria-alice75@example.net	PF	2021-05-18 10:34:00-03	784.620.139-50	1976-05-13	Estação Enzo Gabriel Barros, 98 Serra Do Curral 65953-572 Rocha / SE	01802105
192	Carolina	Vieira	cauadas-neves@example.com	PF	2015-09-27 08:21:00-03	209.316.875-86	1988-06-30	Praia de Duarte, 51 Vila Paquetá 76244-940 das Neves / ES	01651-705
229	Nicolas	Vieira	wpeixoto@example.org	PF	2013-01-20 09:26:00-02	251.807.934-32	1982-10-20	Pátio de Alves Vila Ouro Minas 17435-767 Fernandes / MS	75949393
236	Vitória	Vieira	ucavalcanti@example.com	PF	2020-02-25 08:42:00-03	438.610.279-50	1994-10-30	Lagoa Novaes Vila Nova Cachoeirinha 3ª Seção 63702849 Fernandes / ES	85701095
343	Luiz Gustavo	Vieira	danielada-cruz@example.org	PF	2021-09-20 09:12:00-03	398.712.650-77	1981-09-25	Campo de Silva, 14 Confisco 47012-193 Azevedo / AL	03071741
407	Eduardo	Vieira	vitor-gabrielduarte@example.org	PF	2019-05-18 10:18:00-03	198.203.467-03	1953-04-25	Passarela Marcelo Silva, 1 Esperança 85057312 Ribeiro / DF	11102-836
461	Luiz Felipe	Vieira	evelyn92@example.com	PF	2021-03-12 12:04:00-03	415.639.078-01	2006-09-21	Passarela de Barbosa, 552 Novo Santa Cecilia 24845-782 Fernandes do Campo / DF	26853-378
516	Maria Alice	Vieira	qoliveira@example.org	PF	2019-09-19 12:25:00-03	534.716.029-25	1976-10-26	Estrada Silveira, 31 Conjunto Santa Maria 19867-202 Pereira / ES	14501-313
545	Pedro Henrique	Vieira	nascimentothales@example.com	PF	2019-12-16 07:22:00-03	347.016.892-03	1961-06-19	Travessa de Martins, 768 Vila Aeroporto Jaraguá 29315-900 Sales do Campo / PB	68469-740
600	Luigi	Vieira	henrique46@example.com	PF	2021-01-10 12:35:00-03	421.893.657-91	1958-04-05	Área Duarte, 64 Jardim Felicidade 36546-645 Pires da Mata / MA	47917222
639	João Lucas	Vieira	barbosamariana@example.net	PF	2020-04-16 10:45:00-03	915.028.643-98	1949-04-17	Pátio Barbosa, 69 Alto Caiçaras 70280755 Almeida Alegre / PB	52803-114
712	Thomas	Vieira	leticiada-rocha@example.org	PF	2019-06-16 09:47:00-03	890.562.173-21	2005-11-19	Conjunto de Silveira, 71 Santa Branca 12944-489 da Mata de Pereira / TO	15984-247
773	Vitor Hugo	Vieira	dbarros@example.net	PF	2018-02-27 13:20:00-03	136.784.520-35	1959-04-23	Ladeira Sales, 37 Sagrada Família 39491-441 Castro das Flores / RN	14038881
814	Diogo	Vieira	xribeiro@example.com	PF	2014-09-09 11:55:00-03	805.469.132-70	1960-08-24	Núcleo Lopes, 101 Vila São Gabriel Jacui 50247-386 da Paz Alegre / CE	20079-824
875	Juan	Vieira	helenada-cunha@example.org	PF	2015-01-18 13:07:00-02	529.784.631-55	1980-12-10	Jardim de da Mata, 814 Jatobá 47807379 Sales da Praia / BA	50034-079
948	Maria Vitória	Vieira	portoana-carolina@example.com	PF	2016-09-25 12:39:00-03	473.860.912-22	1971-06-18	Feira Lima Tiradentes 92887274 Azevedo das Flores / PR	03625-157
953	Miguel	Vieira	lucas-gabriel49@example.com	PF	2015-06-11 12:05:00-03	049.571.362-70	1950-12-13	Sítio Emanuella Araújo, 14 Vila Jardim Alvorada 97368-532 Farias / TO	08810805
971	Lucas	Vieira	cfogaca@example.org	PF	2021-02-12 09:24:00-03	203.459.768-00	1968-08-02	Rodovia da Paz, 427 Canadá 00574-813 Cardoso / PE	01900664
994	Davi	Vieira	ana-sophia40@example.net	PF	2015-12-02 10:23:00-02	205.714.936-61	1954-03-19	Colônia João Guilherme Nascimento, 57 Vila Copacabana 91067-853 Mendes / RJ	10730371
102	Laís	da Luz	laura44@example.com	PF	2015-02-28 13:28:00-03	723.810.456-35	1996-03-16	Área de Martins, 1 Pindorama 32716-804 Cunha / AL	46894157
244	Agatha	da Luz	souzalucas-gabriel@example.net	PF	2012-04-20 10:36:00-03	162.975.480-30	1961-12-02	Travessa de Pereira, 4 Conjunto Santa Maria 85409634 Ribeiro da Serra / RN	80189860
417	Cauã	da Luz	da-luzvicente@example.com	PF	2022-10-17 10:56:00-03	170.945.368-01	2000-06-05	Pátio Theo Nunes, 92 Bandeirantes 45313130 Viana / GO	17116-057
473	Maria Sophia	da Luz	isabellarodrigues@example.org	PF	2016-01-02 08:15:00-02	674.890.153-75	2007-04-04	Quadra Sales, 6 João Pinheiro 06829-829 Farias / PA	94233-086
498	Noah	da Luz	emilly25@example.com	PF	2015-08-25 11:46:00-03	271.846.503-44	1968-07-18	Viela Guilherme Peixoto, 460 Vila Atila De Paiva 54579081 Oliveira / PE	56495606
534	Ana Luiza	da Luz	joao-gabrielrocha@example.net	PF	2015-01-25 14:56:00-02	743.205.896-29	1993-09-10	Vale Alves, 14 Jardim Atlântico 62872934 da Cruz do Norte / BA	68532-892
592	Evelyn	da Luz	cgomes@example.com	PF	2014-10-28 12:46:00-02	065.329.748-38	1979-11-24	Conjunto Erick da Cunha Xangri-Lá 91522-348 Costa / AP	76243-469
636	Alícia	da Luz	otavio09@example.com	PF	2016-12-26 13:16:00-02	452.701.836-17	1969-12-18	Alameda Ana Vitória Barros Chácara Leonina 32866-577 Fogaça / AL	36233255
655	Vitória	da Luz	ferreiraluiz-gustavo@example.com	PF	2016-02-11 12:22:00-02	534.291.076-52	1992-12-08	Residencial de Barbosa, 33 Carlos Prates 54142-616 Lima do Oeste / AP	01682-762
714	Calebe	da Luz	cardosoyuri@example.com	PF	2015-06-18 08:20:00-03	416.298.370-40	1955-07-05	Campo Moreira, 3 Miramar 27396-756 Correia / BA	25099-614
716	Emanuel	da Luz	antonioferreira@example.net	PF	2019-05-02 13:21:00-03	214.907.658-67	1960-10-06	Viela Maria Eduarda da Rocha Marilandia 84196-935 Jesus das Pedras / MA	67467-482
722	Mariane	da Luz	emanuelsales@example.net	PF	2018-05-16 10:22:00-03	134.706.295-52	1955-05-15	Distrito Cunha, 99 Belmonte 71696-472 Novaes / RN	28587-599
749	Maria Eduarda	da Luz	costelalavinia@example.net	PF	2018-08-23 09:31:00-03	843.720.915-32	1974-02-17	Residencial Sarah Cardoso, 48 Vila Piratininga 72216-079 Rezende de Costa / MA	64341769
753	Maria Luiza	da Luz	gabrielly88@example.net	PF	2017-07-04 12:05:00-03	123.598.074-04	1979-03-21	Sítio da Rocha, 782 Anchieta 78582065 Carvalho de Silveira / PR	50261-096
785	Daniela	da Luz	sarahda-conceicao@example.net	PF	2021-03-15 12:01:00-03	256.918.430-60	1976-01-28	Trevo Ryan das Neves, 83 Jaraguá 30994887 Cavalcanti Verde / RN	73982-067
821	Clara	da Luz	marianaduarte@example.com	PF	2012-03-12 09:31:00-03	846.231.075-07	2005-02-05	Setor Mirella Carvalho, 660 Atila De Paiva 25278-215 Carvalho da Mata / GO	98938-535
838	Luiz Miguel	da Luz	alicia60@example.com	PF	2013-11-15 14:17:00-02	869.204.573-00	1991-05-10	Chácara Cunha, 98 Conjunto Floramar 62501204 Nascimento do Amparo / PA	94247-539
10	Cecília	da Paz	anthonymendes@example.net	PF	2020-11-03 11:04:00-03	590.412.836-33	1991-03-06	Rua Barbosa Belmonte 20407522 Moura de Santos / MG	91891634
50	Theo	da Paz	heitordas-neves@example.com	PF	2021-11-17 13:16:00-03	284.701.693-78	2005-04-09	Conjunto de da Cunha, 58 Vila Vista Alegre 73062686 Nogueira / PI	12180-103
95	Luiz Gustavo	da Paz	nina11@example.net	PF	2021-09-22 12:00:00-03	852.107.496-49	2003-07-09	Trecho de Duarte, 8 Virgínia 56451275 Campos / SP	67594-724
106	Maria Sophia	da Paz	talmeida@example.net	PF	2010-08-14 12:55:00-03	678.435.901-10	1954-04-15	Condomínio Clarice Porto, 79 Eymard 40757583 Araújo do Amparo / RR	43504963
110	Ana Júlia	da Paz	anthonysilveira@example.com	PF	2020-06-19 13:06:00-03	381.642.570-44	1976-04-01	Travessa Cavalcanti, 26 Vila Satélite 15519413 Campos do Campo / MG	25323-209
143	Amanda	da Paz	maria-luizada-rocha@example.com	PF	2020-04-19 10:29:00-03	879.543.126-82	2000-02-07	Favela de Rocha João Paulo Ii 00138658 Carvalho / PR	77048-875
194	Thomas	da Paz	da-rochabruno@example.org	PF	2016-12-18 12:20:00-02	925.468.301-98	2001-11-15	Aeroporto de Moraes, 89 Lorena 04389826 Rodrigues de da Paz / ES	79586-846
253	Alexia	da Paz	da-motapedro-miguel@example.org	PF	2022-05-27 08:17:00-03	698.435.172-37	1960-05-21	Sítio de Dias, 71 Conjunto Serra Verde 23462-202 Correia / AP	57281-793
347	Diego	da Paz	beatrizdias@example.org	PF	2011-02-03 14:40:00-02	861.793.045-48	1984-12-28	Alameda de Cunha, 3 Boa Esperança 35767424 Pires de Sales / RN	48898293
353	Joaquim	da Paz	pcardoso@example.net	PF	2021-10-19 09:13:00-03	803.219.465-70	1985-09-22	Trecho da Cruz, 5 Ventosa 08912-034 Martins / CE	30966-116
403	Ana Luiza	da Paz	thomasbarbosa@example.com	PF	2020-03-17 10:11:00-03	842.317.906-04	1961-11-13	Loteamento de Almeida, 8 Barão Homem De Melo 1ª Seção 04842-530 Peixoto de Minas / DF	84319-828
500	Caroline	da Paz	correiadaniela@example.net	PF	2016-11-20 11:28:00-02	567.348.209-47	1991-01-23	Distrito Azevedo, 89 Vila Aeroporto 30548410 Barros da Prata / ES	84381061
506	Eloah	da Paz	vitor-hugoda-cruz@example.org	PF	2017-11-25 14:17:00-02	834.570.296-10	1993-12-08	Favela de Santos, 53 Pindura Saia 49020-791 Nogueira da Serra / ES	61725909
776	Manuela	da Paz	kamilly98@example.org	PF	2021-03-14 12:58:00-03	748.530.296-56	1954-12-09	Vereda de Aragão, 69 Vila Santa Rosa 91326-243 Jesus / AL	94563-369
867	Anthony	da Paz	nathanribeiro@example.com	PF	2016-10-14 09:02:00-03	726.903.451-61	1965-04-27	Fazenda de Dias, 20 Nova Cintra 22533-166 Lopes Paulista / RO	40562934
888	Fernanda	da Paz	agatha02@example.org	PF	2016-04-13 08:01:00-03	493.527.810-23	2001-06-12	Núcleo Pietro da Paz, 4 Luxemburgo 49823490 Carvalho / MS	81164-915
894	Miguel	da Paz	luna20@example.org	PF	2019-06-04 09:01:00-03	531.697.248-19	1954-06-02	Avenida Novaes, 29 Baleia 65172-294 Cardoso / AC	20478-520
14	Rafaela	Almeida	vicente09@example.org	PF	2014-08-26 11:14:00-03	271.395.804-05	1966-12-16	Lago Gabrielly Martins, 29 Calafate 26223-583 da Mata / MA	60290-611
16	Nicolas	Almeida	arezende@example.org	PF	2018-04-20 09:01:00-03	142.356.079-52	1994-01-12	Ladeira Lorena Aragão, 5 São Bento 04689671 Moraes da Mata / SP	91014485
48	Marcos Vinicius	Almeida	martinspaulo@example.net	PF	2019-03-09 13:19:00-03	501.482.937-60	1956-01-06	Chácara Amanda Santos, 261 União 52436-400 Fogaça / AP	58403-824
119	Nicolas	Almeida	pereiraana-julia@example.net	PF	2020-01-14 07:20:00-03	021.837.645-62	1980-10-13	Largo da Rosa, 7 Buritis 45383-999 Rodrigues / SE	34227301
128	Carolina	Almeida	emanuellacaldeira@example.net	PF	2018-05-22 09:30:00-03	619.834.257-37	1942-09-08	Esplanada da Luz, 82 Custodinha 80877647 Viana do Campo / SP	90777463
212	Vitória	Almeida	miguel32@example.net	PF	2021-04-09 12:40:00-03	381.576.940-00	2003-04-02	Esplanada Castro, 96 Padre Eustáquio 77331429 Pereira / GO	88489-764
227	Vitor	Almeida	valentinacastro@example.org	PF	2022-12-27 10:30:00-03	038.572.416-08	1964-07-12	Lagoa Rebeca Costela Acaba Mundo 54722189 da Costa / MG	60794-612
297	Valentina	Almeida	gomesraul@example.com	PF	2018-08-11 10:45:00-03	513.269.804-24	1952-03-03	Residencial de Oliveira, 12 Vila Nossa Senhora Do Rosário 05139-024 Farias / PE	10207922
312	Ana Laura	Almeida	fernandesana-livia@example.com	PF	2022-07-12 13:34:00-03	654.098.213-51	1954-10-26	Conjunto Fernandes Novo Glória 18748994 Costa / RO	29021919
391	Anthony	Almeida	fda-conceicao@example.org	PF	2020-06-21 08:39:00-03	083.415.276-26	1999-10-03	Loteamento Marcelo Melo, 4 Brasil Industrial 66720124 Araújo Grande / DF	04554-217
532	Heloísa	Almeida	lorena15@example.net	PF	2021-07-13 11:56:00-03	054.897.126-94	2005-07-01	Lago Cardoso, 25 Venda Nova 91678327 Gonçalves / SC	67892719
647	Augusto	Almeida	henriqueda-rocha@example.com	PF	2018-12-19 11:08:00-02	592.631.874-28	1964-01-12	Recanto de da Conceição, 7 Senhor Dos Passos 18905997 da Costa de Nogueira / PB	01814-432
832	Luigi	Almeida	bryanviana@example.com	PF	2012-12-25 13:27:00-02	257.369.184-55	1983-02-09	Colônia Ribeiro, 85 Santo André 30119-374 da Paz Verde / PA	93983938
856	Nicole	Almeida	guilhermesales@example.com	PF	2015-06-24 12:02:00-03	259.406.318-51	1961-10-24	Setor Ana Laura Moura, 65 Mangabeiras 00902-688 Aragão Alegre / PR	39587-897
883	Sabrina	Almeida	maria-cecilia82@example.org	PF	2015-09-05 13:18:00-03	389.047.162-50	1998-07-07	Conjunto Aragão Serrano 88509069 Gomes / PB	26119727
915	Yago	Almeida	ana-sophia34@example.org	PF	2018-12-25 10:03:00-02	452.018.697-85	1971-12-30	Pátio Ana Julia Martins, 57 Comiteco 50652-848 da Cruz do Norte / MA	03756332
4	Lara	Aragão	joao-lucascosta@example.com	PF	2017-04-06 09:28:00-03	096.482.537-65	1986-05-01	Estação Cauã Santos, 20 Vila Mantiqueira 34579-230 da Cruz / MT	19720-769
7	Matheus	Aragão	barbosajuan@example.net	PF	2018-09-12 10:08:00-03	859.406.321-06	1985-11-13	Estação de da Rocha, 59 Aeroporto 24555159 Barbosa / GO	29456824
177	Diogo	Aragão	bfreitas@example.org	PF	2022-10-15 13:31:00-03	470.693.521-07	1986-11-20	Ladeira de Azevedo, 2 Eymard 34582863 Silva / AP	50246702
208	João Gabriel	Aragão	cunhamarcos-vinicius@example.com	PF	2013-05-12 10:48:00-03	963.085.247-00	1987-10-01	Campo de Teixeira, 93 Biquinhas 77899-799 Duarte de Cavalcanti / PI	90069673
264	Gustavo Henrique	Aragão	joao-vitorfernandes@example.net	PF	2020-03-19 11:13:00-03	375.210.948-32	1958-03-04	Passarela de Carvalho Teixeira Dias 85975-530 da Rocha / MT	38755-229
320	Erick	Aragão	da-costaemanuel@example.net	PF	2017-08-26 10:14:00-03	318.076.529-12	2001-09-24	Vale Fogaça, 13 Vila Mantiqueira 93751-775 Souza / DF	37217800
413	Emanuelly	Aragão	ana-beatriz92@example.org	PF	2014-05-20 08:41:00-03	075.329.618-77	1974-05-24	Residencial de Silva, 73 Santa Lúcia 41532-788 Fogaça / DF	50743388
591	Luiz Gustavo	Aragão	costaemanuella@example.org	PF	2022-05-19 08:46:00-03	724.856.910-02	1996-06-21	Travessa da Cunha, 63 Engenho Nogueira 20452244 Gonçalves / AC	23882-749
709	Isabelly	Aragão	xpereira@example.net	PF	2018-09-07 08:13:00-03	670.439.581-48	1949-05-19	Esplanada Renan Araújo, 17 Vila Tirol 83507-776 Vieira / DF	37884316
710	Laís	Aragão	das-nevesluiza@example.com	PF	2019-06-26 09:44:00-03	854.396.107-66	1972-12-11	Jardim Valentina Gomes João Paulo Ii 90979-552 Campos de Melo / MS	03166-379
765	Beatriz	Aragão	valentinacunha@example.com	PF	2019-05-27 09:48:00-03	307.659.184-39	1966-03-14	Residencial Ian Almeida, 975 Jardim América 75948-364 Moura / GO	59717-836
787	Rafael	Aragão	hpinto@example.net	PF	2017-10-10 09:04:00-03	824.563.719-09	1986-07-24	Esplanada Araújo, 37 Vila Califórnia 85418025 da Costa de Fogaça / SP	22250134
855	Letícia	Aragão	da-rochalivia@example.org	PF	2017-02-25 10:28:00-03	467.310.895-75	1992-10-02	Rua de Silveira, 314 Vera Cruz 23815755 Gonçalves Alegre / AL	54516237
932	João	Aragão	carvalhojoao-lucas@example.net	PF	2015-06-21 09:06:00-03	809.674.153-57	1994-07-31	Rua Nicolas Peixoto, 73 Ernesto Nascimento 17498446 Nogueira / MA	26474200
986	Emanuelly	Aragão	lpinto@example.com	PF	2016-01-23 10:45:00-02	931.462.507-99	2006-11-06	Pátio Pedro Henrique Rodrigues, 9 Vila Fumec 49827910 Gomes / AM	36694-446
18	Leandro	Araújo	brenda60@example.net	PF	2021-07-18 12:07:00-03	160.275.498-58	1991-10-15	Lago de da Mota, 7 Pindorama 62566-272 Costela do Oeste / CE	56676-337
112	Pedro Lucas	Araújo	bernardo60@example.net	PF	2021-07-11 09:15:00-03	809.734.215-41	1986-02-11	Colônia Ana Beatriz Freitas, 6 Atila De Paiva 51908198 da Rosa / RJ	41121-283
234	João Pedro	Araújo	marceloda-conceicao@example.org	PF	2021-03-08 10:43:00-03	510.674.982-49	1971-12-25	Núcleo Cavalcanti, 483 São Vicente 29422490 da Rosa / RR	64483022
246	Lorena	Araújo	ribeironatalia@example.org	PF	2019-05-23 12:55:00-03	970.123.548-79	1966-04-08	Chácara Silveira, 7 Vila Cloris 88301-767 Jesus da Serra / SP	02240596
251	Pietra	Araújo	fogacaisadora@example.com	PF	2013-07-11 11:28:00-03	827.369.401-13	1996-10-13	Loteamento Melo, 85 São Sebastião 27542-399 da Luz de Peixoto / AC	72383779
361	Clarice	Araújo	barbaraalmeida@example.net	PF	2018-01-08 12:30:00-02	306.548.291-60	1946-09-29	Via Azevedo, 79 Santo Agostinho 02873409 Moraes da Mata / PE	57218048
378	Pedro Lucas	Araújo	srezende@example.com	PF	2018-03-18 09:36:00-03	325.496.710-07	1943-10-17	Parque Luna Nunes, 2 Novo Ouro Preto 54121906 Vieira do Oeste / AM	58010319
489	Marcela	Araújo	bernardocampos@example.org	PF	2019-08-04 10:50:00-03	529.308.476-38	1958-11-23	Rua de da Rosa, 8 Mantiqueira 45168-327 Cardoso / PR	46872-672
491	Mariana	Araújo	nsantos@example.org	PF	2020-09-03 11:33:00-03	645.189.273-46	1979-09-27	Núcleo de Jesus, 28 Serrano 65469765 Moreira / PR	59657-825
658	Nicolas	Araújo	marcela88@example.com	PF	2017-02-07 10:10:00-02	163.298.047-96	1967-08-22	Setor Lima, 93 Vila Nova Gameleira 2ª Seção 30697086 Moura / RN	32199475
737	Pedro Miguel	Araújo	melissagomes@example.org	PF	2021-01-28 11:26:00-03	874.261.395-73	1980-04-02	Condomínio Bruna da Mota, 628 Das Industrias I 27208-640 Nascimento Alegre / TO	95890981
809	Ana Carolina	Araújo	pauloaraujo@example.org	PF	2021-05-18 13:15:00-03	847.659.210-85	1963-11-30	Praça de Moura, 3 Monte São José 73520387 Rezende do Amparo / MG	80456-424
860	Igor	Araújo	wgomes@example.net	PF	2021-04-15 11:16:00-03	647.258.031-62	1988-06-06	Estação de da Rocha, 65 Distrito Industrial Do Jatoba 05154-328 Novaes do Amparo / MA	58065269
921	Anthony	Araújo	martinsian@example.net	PF	2021-09-03 12:52:00-03	824.319.657-91	1949-06-04	Rua João Felipe da Mata, 7 Piratininga 85495859 Moreira de Rodrigues / AC	12376047
63	Manuela	Azevedo	laura78@example.net	PF	2019-04-10 09:23:00-03	379.805.641-20	1993-04-21	Fazenda Duarte Cabana Do Pai Tomás 61838-337 da Mota / AM	93898969
149	Júlia	Azevedo	samuel74@example.com	PF	2020-06-25 09:32:00-03	591.283.067-59	2004-10-18	Praça Diogo Costa, 3 São João 30181-466 Martins do Amparo / AL	45650110
155	Anthony	Azevedo	leandro14@example.com	PF	2013-08-22 10:27:00-03	734.596.812-09	1972-07-13	Pátio de Cavalcanti, 46 Cardoso 93081-065 da Rocha / MT	10764-469
169	Cauã	Azevedo	amonteiro@example.com	PF	2015-07-01 10:19:00-03	523.691.478-73	1998-11-18	Ladeira da Mata, 4 Colégio Batista 47750235 Barbosa / AC	29074-591
174	Manuela	Azevedo	helena76@example.net	PF	2017-04-21 12:02:00-03	713.650.289-68	1965-09-28	Loteamento da Costa, 6 Braúnas 56159-268 Fernandes da Praia / CE	13715347
442	Alice	Azevedo	nicolefreitas@example.net	PF	2022-12-17 10:24:00-03	942.076.813-96	1955-07-27	Rua de da Conceição, 96 Juliana 48437-460 Ramos da Mata / PR	41703129
626	Luiz Fernando	Azevedo	ecavalcanti@example.com	PF	2022-07-25 07:12:00-03	481.932.570-14	1988-08-17	Alameda Ryan Jesus Vila Santa Rosa 97040-741 da Rocha / RS	66800-272
661	Otávio	Azevedo	marcos-vinicius93@example.com	PF	2013-09-11 10:40:00-03	512.784.390-05	1960-02-18	Rodovia Emanuella Souza, 4 Saudade 31563951 Silva / BA	26189090
930	Ana	Azevedo	mouravicente@example.org	PF	2018-04-03 08:06:00-03	370.259.614-34	1976-01-15	Favela de Fogaça, 9 Trevo 80538819 Pinto / SC	79261182
181	João Gabriel	Oliveira	joao-pedro84@example.com	PF	2021-06-08 12:03:00-03	956.478.320-83	1968-01-02	Favela de Rodrigues Vila Do Pombal 83837-731 da Costa / SC	52099-862
129	Sarah	Barbosa	martinsvitor-hugo@example.com	PF	2013-01-08 13:31:00-02	903.421.678-04	1972-01-15	Parque Leandro Campos, 356 Vila São Gabriel Jacui 87146-556 Oliveira Paulista / AC	75315066
184	Lavínia	Barbosa	laraalmeida@example.org	PF	2018-11-12 11:09:00-02	386.952.074-47	1963-10-17	Estação Teixeira, 97 Vera Cruz 69838548 Pires do Campo / RJ	61548005
300	Ana Vitória	Barbosa	liviateixeira@example.org	PF	2021-06-24 12:39:00-03	275.089.136-12	1981-02-25	Setor de Viana, 972 Padre Eustáquio 88598-612 Ribeiro de Goiás / PA	80902-628
375	Erick	Barbosa	rodrigo94@example.org	PF	2019-06-22 12:56:00-03	203.795.486-74	1967-12-15	Vale de Vieira, 27 Ribeiro De Abreu 39530-310 Martins / RN	82725835
443	João Lucas	Barbosa	isabella02@example.net	PF	2022-01-03 08:42:00-03	735.489.160-66	1948-01-11	Fazenda Vicente Gonçalves, 46 Nossa Senhora Da Aparecida 46869-970 Farias / PE	52387994
590	Daniel	Barbosa	barbosaalexia@example.org	PF	2018-10-09 08:33:00-03	870.514.932-05	1959-03-02	Favela Luiz Felipe Novaes Mala E Cuia 51525-067 Barbosa de Gonçalves / RN	03233-139
686	Thiago	Barbosa	maria81@example.org	PF	2021-05-26 07:49:00-03	264.395.871-37	1944-12-09	Alameda Lima Vila Minaslandia 47044993 Ferreira / BA	26574-404
726	André	Barbosa	zcaldeira@example.org	PF	2019-11-06 12:14:00-03	951.234.870-50	1996-12-09	Estrada Maria Vitória Vieira, 16 Universo 77552-552 Souza do Sul / DF	98191742
851	Leandro	Barbosa	raquel68@example.org	PF	2022-05-02 07:45:00-03	153.807.926-77	1983-03-20	Distrito Sabrina Araújo, 33 Pindura Saia 95049-744 Moraes de Minas / MG	11027-566
874	Murilo	Barbosa	jcaldeira@example.com	PF	2021-11-20 07:46:00-03	749.128.506-67	1986-05-28	Largo Cardoso, 4 Novo Santa Cecilia 21109235 Cardoso / RO	60767-792
919	Raul	Barbosa	csouza@example.com	PF	2014-10-18 09:17:00-03	716.940.325-07	2006-07-25	Viaduto Olivia Barbosa, 10 Vila Boa Vista 09314-990 Fernandes da Mata / PE	10493-845
988	Clara	Barbosa	alexiada-rosa@example.net	PF	2021-02-22 11:02:00-03	821.375.604-53	1963-12-31	Passarela Luiz Otávio Nascimento Conjunto Jardim Filadélfia 42086678 Santos / MA	83565-860
3	Breno	Cardoso	rsouza@example.net	PF	2013-01-05 11:13:00-02	364.982.015-33	1975-05-30	Feira Aragão, 41 Marmiteiros 56012309 Araújo / AM	99161510
21	Davi Luiz	Cardoso	eduardoda-costa@example.com	PF	2016-07-10 09:29:00-03	716.430.895-01	1957-04-13	Morro Pedro Miguel Pinto, 9 São Luiz 98347-897 Fogaça de Goiás / AC	91263340
43	Vitor Gabriel	Cardoso	lucas-gabriel73@example.com	PF	2016-09-03 12:08:00-03	607.584.319-10	1971-07-15	Recanto Gustavo Henrique Dias, 64 Vila Da Paz 92813-481 da Costa / MT	59276367
77	Mirella	Cardoso	maite60@example.net	PF	2019-02-01 11:17:00-02	324.870.195-14	2001-08-15	Vila Maria Fernanda Barbosa, 10 Virgínia 52639830 Ramos / PI	99555-106
109	Maria Cecília	Cardoso	sofia85@example.net	PF	2022-03-16 10:53:00-03	092.378.516-77	1978-12-28	Sítio de da Mota Engenho Nogueira 86201-053 Correia das Pedras / DF	11243-236
210	Lucas	Cardoso	maria-alice27@example.net	PF	2018-11-15 12:36:00-02	189.342.650-51	1953-03-26	Vila Sales, 56 São Lucas 72371269 das Neves / PA	44811-766
217	Vitor	Cardoso	rezendeisis@example.org	PF	2018-09-05 11:44:00-03	927.154.038-79	1971-09-18	Núcleo Peixoto, 29 Beija Flor 12707682 Cunha / RR	11883-390
218	Enzo	Cardoso	martinsjoao-miguel@example.com	PF	2019-10-01 13:05:00-03	529.140.378-05	2005-10-10	Distrito Clarice da Luz, 35 Lourdes 24553496 Lopes do Sul / SC	42430-747
241	Joaquim	Cardoso	cardosomaysa@example.net	PF	2021-02-26 09:19:00-03	136.942.087-04	1979-03-07	Trecho Yago Dias, 20 Jardim América 07951411 da Mota / ES	71337392
263	Breno	Cardoso	vsantos@example.org	PF	2017-12-27 11:22:00-02	180.964.753-39	1942-09-02	Condomínio Gabrielly Costela, 485 São Bento 04770068 Oliveira da Serra / AP	83728117
270	Natália	Cardoso	barrosclarice@example.com	PF	2019-09-05 13:38:00-03	304.872.619-50	1994-07-04	Praça de Ferreira, 5 Flamengo 27068-714 Azevedo do Amparo / MA	21373248
307	Augusto	Cardoso	fernanda99@example.org	PF	2017-02-10 14:42:00-02	792.651.043-25	2005-08-15	Distrito Erick Cavalcanti, 36 Mangabeiras 66325210 da Conceição / RN	38243-006
336	Mirella	Cardoso	marcelo05@example.org	PF	2020-10-18 10:03:00-03	782.156.043-07	1960-02-23	Fazenda Souza, 413 Padre Eustáquio 22283686 Farias do Norte / PR	51618919
468	Kevin	Cardoso	vitor29@example.com	PF	2021-08-27 10:19:00-03	045.718.296-20	1994-01-01	Parque Costa, 322 Santa Sofia 25665-372 Alves / CE	82355-894
481	Yago	Cardoso	vitorfogaca@example.net	PF	2020-04-14 13:19:00-03	364.872.105-44	1964-10-11	Estação Correia, 1 Santa Maria 51863961 Nascimento / PR	41812110
552	Júlia	Cardoso	ssilva@example.net	PF	2018-06-17 11:15:00-03	257.601.483-62	1954-09-27	Área João Felipe Ferreira, 824 Dom Cabral 21297581 Fernandes / TO	08094-852
568	Maria Fernanda	Cardoso	yago53@example.org	PF	2020-08-22 12:21:00-03	869.215.347-82	1960-08-13	Vale Silveira, 3 Nova America 17600-099 da Conceição da Praia / PB	99471851
597	Lucas	Cardoso	julia00@example.net	PF	2019-06-19 09:21:00-03	973.281.605-86	1980-04-29	Campo Otávio Gomes, 11 Barão Homem De Melo 3ª Seção 48152-500 das Neves do Campo / MS	52389135
667	Ana	Cardoso	maria-juliasouza@example.com	PF	2022-03-06 12:06:00-03	312.047.986-13	1988-01-23	Trecho Luigi Cavalcanti, 59 Vila Pilar 16218326 Pinto / PR	90177-078
669	Cauã	Cardoso	rebecada-rosa@example.net	PF	2016-04-15 11:26:00-03	376.248.510-08	1942-11-24	Setor Olivia Monteiro, 217 Santa Isabel 10045-601 Jesus / MS	99364-926
685	Enzo	Cardoso	mirella43@example.com	PF	2021-02-27 12:29:00-03	856.970.321-02	1984-07-24	Passarela de Azevedo, 1 Santa Monica 92384-193 Caldeira Verde / MG	54474894
707	Caio	Cardoso	maria-julia07@example.com	PF	2016-10-22 12:14:00-02	832.146.759-82	1945-09-22	Travessa de Costela Santa Lúcia 58697-782 Cavalcanti Alegre / PR	71436-266
735	Rafael	Cardoso	idas-neves@example.org	PF	2012-12-03 11:05:00-02	183.729.546-82	1998-04-07	Esplanada Nunes, 46 Etelvina Carneiro 03683-959 Pires / BA	35352-960
790	Clarice	Cardoso	emanuella58@example.net	PF	2014-07-03 13:49:00-03	351.874.609-00	1998-10-03	Rua de Moraes, 8 Vila Batik 22249-412 Cunha Paulista / PE	00506-259
798	Calebe	Cardoso	juanmelo@example.org	PF	2015-11-21 08:49:00-02	063.872.549-65	1982-12-22	Praia de da Rosa, 89 Vila Santa Monica 1ª Seção 87063490 Gonçalves / MS	67600436
968	Fernando	Cardoso	isabella58@example.org	PF	2018-02-01 14:14:00-02	523.890.467-38	1999-07-10	Morro Lopes Copacabana 32525-631 da Cruz dos Dourados / SP	86938-977
74	Maria Alice	Correia	ugomes@example.com	PF	2016-07-26 07:37:00-03	604.238.791-03	1957-04-06	Sítio Juan Nascimento, 7 Apolonia 15044000 da Costa / RN	71920821
81	Laís	Correia	marcela49@example.com	PF	2021-04-03 10:18:00-03	076.351.942-16	2006-07-10	Residencial de Nascimento, 46 Vila Santo Antônio 34127-554 Mendes de Farias / CE	25372-189
137	Cauã	Correia	da-pazemanuella@example.org	PF	2017-01-06 12:34:00-02	819.072.346-40	1963-05-07	Travessa de das Neves, 34 Nova Granada 25332-967 Caldeira Verde / CE	41060234
372	Brenda	Correia	qgoncalves@example.org	PF	2013-11-27 11:03:00-02	345.926.017-34	1947-08-30	Feira de Moura Vila Antena 67354401 Caldeira do Amparo / MG	11580065
392	Isabella	Correia	joao-lucasfreitas@example.org	PF	2015-12-26 13:09:00-02	560.217.498-20	2000-03-02	Jardim Yago das Neves, 49 Vila São Rafael 31159338 Mendes / BA	18306-600
408	João	Correia	carvalhoemilly@example.com	PF	2021-10-15 07:47:00-03	213.659.487-73	1943-06-22	Parque Barros, 87 Caetano Furquim 21534-413 Jesus / RN	75390263
428	Pedro Henrique	Correia	jlopes@example.net	PF	2019-11-27 11:29:00-03	160.872.359-30	1965-03-10	Núcleo Pinto, 562 Cidade Nova 40869328 Lopes do Sul / AC	69003-972
536	Maysa	Correia	eduardo67@example.net	PF	2020-12-09 11:29:00-03	358.049.716-20	1953-06-26	Morro Sofia da Mata, 36 Casa Branca 86256077 Mendes / RJ	02785728
574	Vitória	Correia	jduarte@example.org	PF	2014-03-05 13:10:00-03	951.247.836-64	1949-04-19	Lago de da Mata, 489 São Vicente 89030914 Costa / RR	13347642
625	João	Correia	claralopes@example.org	PF	2021-01-09 08:36:00-03	401.287.935-97	1987-08-08	Fazenda Almeida, 32 Vila Batik 19866-033 Teixeira / MS	61410-865
900	Levi	Correia	cfarias@example.org	PF	2021-08-06 09:29:00-03	809.537.462-83	1961-08-02	Jardim Rocha, 72 Novo Glória 79010-844 Novaes / RR	92278668
153	Luiz Gustavo	Costela	rafael61@example.net	PF	2014-09-12 13:45:00-03	048.372.651-62	1945-12-22	Conjunto de Costa, 4 Granja De Freitas 18338464 Rocha de Peixoto / DF	03524308
165	Ana Carolina	Costela	davi-lucas33@example.com	PF	2020-01-23 09:15:00-03	913.647.852-00	1959-04-27	Residencial Clarice Caldeira São Luiz 36792-608 Santos / ES	02181-326
252	Agatha	Costela	hcampos@example.org	PF	2019-07-18 09:56:00-03	549.132.680-15	1972-04-07	Vereda Moura, 6 Nossa Senhora Da Conceição 92168616 da Mata das Flores / AP	59625-368
319	Cecília	Costela	nicolasjesus@example.com	PF	2021-08-18 12:41:00-03	396.721.085-59	1990-01-05	Praça de Duarte, 28 Vila São Rafael 62758847 Pereira do Sul / SC	57267113
486	Davi Lucca	Costela	araujogustavo-henrique@example.org	PF	2015-04-23 10:18:00-03	869.345.027-10	2002-02-09	Parque Gomes, 732 Nossa Senhora De Fátima 25354-774 da Mota do Amparo / SP	93250-071
538	Sophie	Costela	nicolas99@example.com	PF	2022-11-25 08:24:00-03	812.964.530-06	1969-06-10	Chácara Ramos, 91 Vila Jardim Leblon 67405-337 Gomes do Sul / MA	72101-372
642	Enzo Gabriel	Costela	da-cruzbryan@example.org	PF	2019-06-26 09:58:00-03	493.825.617-73	1947-04-24	Campo Sales, 66 Santa Tereza 16101-633 Sales / TO	17468-663
728	Gabrielly	Costela	maite10@example.org	PF	2020-11-23 09:34:00-03	874.509.261-30	2004-03-30	Conjunto de Martins, 96 Vila Paquetá 28412794 Ribeiro / RS	47587350
745	Marcela	Costela	klima@example.org	PF	2017-04-23 12:36:00-03	204.953.816-24	1971-03-13	Viaduto Arthur Nascimento, 45 João Alfredo 55135-284 Araújo da Praia / PB	44587-274
795	Bernardo	Costela	correiamaria-julia@example.net	PF	2022-02-25 12:08:00-03	841.052.739-14	1977-12-09	Rodovia de Barros, 48 Esplanada 39552867 da Cunha Verde / RS	86841-476
958	Daniel	Costela	mdias@example.org	PF	2018-10-09 12:05:00-03	218.569.047-76	1977-02-17	Parque da Mota, 27 Beija Flor 84721055 Ramos da Serra / MS	57105-880
26	Augusto	Fogaça	fernandesmaria-julia@example.net	PF	2012-08-12 13:34:00-03	435.821.079-14	1997-01-27	Setor de Moraes, 62 Gutierrez 90925544 Sales das Flores / RN	20290-725
455	Luna	Fogaça	maria-julia65@example.org	PF	2017-08-24 07:26:00-03	396.150.427-06	1970-07-31	Aeroporto Luiz Miguel Nascimento, 936 Bom Jesus 73993124 Correia / MA	69265010
512	Vitória	Fogaça	teixeiraluiz-gustavo@example.net	PF	2020-05-12 09:40:00-03	260.145.378-80	2005-06-05	Via Noah Fernandes, 78 Tres Marias 69283-118 Mendes das Flores / RR	86720-928
689	Gabriel	Fogaça	marinasilva@example.org	PF	2014-04-15 08:08:00-03	580.342.167-71	1971-01-31	Largo Alves Solimoes 35556-607 Vieira de Moraes / PE	20921-329
718	Stella	Fogaça	carvalhocamila@example.net	PF	2018-03-09 11:52:00-03	241.905.873-97	1990-11-21	Rua de da Conceição, 43 Marieta 1ª Seção 91451-022 Ribeiro da Prata / AP	91322358
766	Ana Vitória	Fogaça	salesisabella@example.com	PF	2014-09-07 10:15:00-03	943.170.528-14	1974-05-01	Área de da Rosa, 58 Estrela Do Oriente 08916-900 Fernandes do Campo / AM	66310967
775	Kevin	Fogaça	mmelo@example.com	PF	2011-03-19 11:55:00-03	921.738.064-40	1993-05-07	Conjunto da Cruz, 17 Gameleira 27867-967 Costa / MG	15510174
86	Rebeca	Freitas	da-rosamaysa@example.com	PF	2016-03-05 10:55:00-03	468.571.930-10	2007-01-11	Campo Fogaça, 382 Vila Ecológica 17362-108 Carvalho / SE	92293551
173	Marcela	Freitas	clarada-mota@example.com	PF	2021-05-12 11:45:00-03	379.042.651-25	1987-06-27	Setor Rezende, 485 Nossa Senhora Da Aparecida 95882713 Moreira de Ribeiro / SC	42074-019
215	Lívia	Freitas	ydias@example.org	PF	2020-04-10 11:14:00-03	194.703.268-22	1969-11-29	Trevo Isis Costa, 565 Madre Gertrudes 92733172 da Mata / MS	43218579
275	Julia	Freitas	da-cruzisabella@example.com	PF	2020-05-02 09:18:00-03	026.754.139-25	1970-09-01	Condomínio de Pires, 98 Suzana 20099-665 Pires da Praia / RJ	56335095
348	João Gabriel	Freitas	pintothales@example.net	PF	2018-03-03 08:58:00-03	681.324.790-50	1991-02-25	Loteamento de da Luz, 89 Caiçaras 58869-612 Farias / AM	31770-420
385	João Felipe	Freitas	pedro-lucassouza@example.com	PF	2018-08-06 09:05:00-03	134.729.850-97	1944-11-04	Passarela Nicole da Mota, 1 Vila Pinho 18178711 Costela da Mata / PI	04123732
435	Davi	Freitas	juliacastro@example.org	PF	2011-02-02 09:14:00-02	649.270.381-22	1970-09-02	Passarela de Almeida, 88 Conjunto Celso Machado 52242-507 Nascimento / RR	17882-080
474	Otávio	Freitas	joao-pedro86@example.net	PF	2020-01-12 07:03:00-03	164.903.785-66	1976-09-25	Quadra Araújo, 301 Buraco Quente 28418-860 Lopes / RJ	62436-426
477	Otávio	Freitas	xmoura@example.net	PF	2022-08-20 11:19:00-03	978.205.314-79	1967-10-13	Vereda Marcelo Souza, 1 Alto Das Antenas 12225-558 Melo Verde / ES	19130-426
607	Heloísa	Freitas	cramos@example.net	PF	2015-12-04 09:48:00-02	026.348.951-51	1983-04-15	Condomínio Sophia da Mata Antonio Ribeiro De Abreu 1ª Seção 29820-081 da Mota / PA	76013101
687	Gustavo Henrique	Freitas	santospietra@example.net	PF	2021-12-14 07:04:00-03	304.921.786-31	1952-04-02	Colônia de Pires, 733 Alto Caiçaras 31721120 Pereira de Farias / RN	88959-636
807	Lorena	Freitas	da-costagabriela@example.com	PF	2022-07-28 13:18:00-03	372.841.605-35	1980-01-30	Rodovia Monteiro, 80 Vila Piratininga 08695890 Nunes / RR	60173281
869	Eduarda	Freitas	gcardoso@example.org	PF	2021-04-09 10:01:00-03	712.643.508-80	1979-05-03	Morro Novaes, 7 Estrela 33844133 Azevedo / MG	68219-707
878	Guilherme	Freitas	murilomoreira@example.com	PF	2012-05-16 11:13:00-03	561.034.879-00	2001-02-08	Lagoa Moreira Bandeirantes 52917-532 Dias do Oeste / AP	10183184
891	Gustavo Henrique	Freitas	oliveirahenrique@example.net	PF	2012-02-20 11:42:00-02	126.094.378-03	1982-02-05	Parque de da Paz, 8 Vila Dos Anjos 71773-667 das Neves do Galho / DF	45295-653
923	Lucas	Freitas	isaac81@example.com	PF	2021-05-05 10:11:00-03	294.613.508-05	2001-08-15	Jardim de Ramos, 220 Alto Barroca 55584857 Barbosa / TO	62512714
997	Raquel	Freitas	yfreitas@example.com	PF	2020-11-20 10:29:00-03	485.091.726-76	1979-06-08	Trecho de Araújo, 816 Cruzeiro 74452-919 Silva da Praia / AP	00427644
2	Luigi	Martins	da-motaisabel@example.org	PF	2020-03-27 10:05:00-03	287.069.351-68	1979-10-27	Sítio Melo, 939 Túnel De Ibirité 71965-934 da Mota / TO	09471122
65	Daniela	Martins	maria-julia15@example.org	PF	2018-08-26 09:33:00-03	486.573.902-56	1972-04-23	Viaduto de da Costa, 839 Marçola 92964312 Pires / PI	20479-151
145	Carolina	Martins	paulosantos@example.org	PF	2011-03-03 10:41:00-03	395.412.607-99	1955-07-05	Jardim Nathan Nogueira, 11 Aarão Reis 38082994 da Cruz / RJ	07948-351
176	Alexia	Martins	isadorasilveira@example.net	PF	2019-04-20 11:06:00-03	816.732.495-82	1968-07-31	Residencial Rocha Paquetá 32152982 das Neves de das Neves / PI	72098-448
200	Amanda	Martins	andreda-rosa@example.org	PF	2015-05-17 07:24:00-03	627.984.015-85	1955-02-28	Estação Enzo Duarte, 66 Lagoinha 39361698 Jesus / AC	94558596
209	Benjamin	Martins	pintomaria-julia@example.net	PF	2021-02-07 09:38:00-03	718.243.965-46	1955-04-13	Estação de Pinto, 135 Vila Fumec 67616404 da Conceição / MG	53805660
216	Davi Lucca	Martins	barbosapedro@example.org	PF	2019-12-06 08:03:00-03	679.425.031-43	1955-08-10	Rua de Sales, 9 Álvaro Camargos 12577816 Vieira / SE	25721-707
220	Otávio	Martins	ncardoso@example.net	PF	2017-08-17 08:51:00-03	016.892.735-77	1991-11-25	Campo Stella Alves, 94 Alto Das Antenas 89423334 Cardoso / ES	10725-665
245	Marcos Vinicius	Martins	aragaoyasmin@example.com	PF	2018-06-15 08:38:00-03	693.754.201-16	1994-02-19	Campo Vinicius da Cruz, 6 Varzea Da Palma 60132623 Rodrigues / RR	36206809
272	Isadora	Martins	karagao@example.com	PF	2021-08-25 07:48:00-03	107.643.259-06	1943-09-06	Rua de Novaes Maria Tereza 97518804 Rodrigues / AC	35780004
337	Lorena	Martins	elisadias@example.net	PF	2017-08-03 11:16:00-03	094.328.765-00	1999-03-16	Rua Kamilly Moura, 21 Vista Alegre 44980682 Lima / RR	13920715
355	Clara	Martins	davi92@example.org	PF	2022-10-22 11:50:00-03	452.370.619-07	1993-12-01	Residencial Novaes, 62 Vila União 94123-530 Silveira / AM	05790922
579	Catarina	Martins	pintobeatriz@example.net	PF	2018-10-03 13:00:00-03	319.462.508-05	1981-10-03	Conjunto de Rodrigues, 1 Vila Independencia 1ª Seção 37495-506 Ferreira de Minas / AM	08026207
711	Ian	Martins	pintostephany@example.com	PF	2022-12-08 11:47:00-03	274.086.351-90	1951-01-25	Pátio Luiz Henrique Lopes, 97 Marmiteiros 88332668 Silva da Prata / GO	01697859
723	Diego	Martins	mirellabarros@example.com	PF	2013-12-02 12:38:00-02	859.234.760-29	1982-12-01	Sítio de da Luz, 12 Casa Branca 78998124 Pereira / DF	93720793
858	Enzo Gabriel	Martins	rafaela90@example.net	PF	2018-11-28 09:24:00-02	314.627.580-53	1956-10-19	Viela Nunes, 38 Pousada Santo Antonio 79028676 Jesus de Rodrigues / AL	63811868
892	Daniela	Martins	gustavo-henrique28@example.org	PF	2015-08-05 09:19:00-03	182.973.605-12	1991-03-20	Travessa Lima, 30 Novo São Lucas 61722185 Ferreira / DF	03412726
910	Renan	Martins	vgoncalves@example.org	PF	2011-11-03 14:42:00-02	628.749.350-00	1985-03-07	Viela Rezende, 72 Acaba Mundo 81250-567 Cardoso da Praia / MS	28143660
950	Rafaela	Martins	ocosta@example.com	PF	2020-10-23 11:33:00-03	301.859.672-21	1952-01-25	Esplanada de Lima, 792 Universitário 17334-458 Fernandes Paulista / PB	00167-046
961	Yago	Martins	ferreiradavi@example.com	PF	2020-05-13 08:41:00-03	061.472.839-87	1994-08-02	Estrada Rezende, 10 Parque São Pedro 88844755 Pereira Verde / GO	07616-295
53	Maria Alice	Moreira	anthony00@example.net	PF	2011-05-24 12:30:00-03	436.782.901-40	1995-07-12	Estação Alves, 684 Vila Pilar 63522729 Dias do Sul / AL	90281-645
58	Bruno	Moreira	rodrigofogaca@example.com	PF	2017-11-02 10:54:00-02	603.597.248-92	2005-01-18	Recanto de Souza, 56 Vila Jardim Leblon 30799-297 Martins das Pedras / RJ	86635643
94	Sarah	Moreira	bryansilva@example.org	PF	2011-09-09 12:00:00-03	746.901.258-30	1987-08-22	Residencial Vinicius Gonçalves, 5 Vila Maloca 54353333 Costa da Prata / GO	56080861
178	Maria Clara	Moreira	limagustavo@example.com	PF	2020-07-17 13:24:00-03	678.254.103-35	2003-03-27	Travessa Oliveira, 65 Bandeirantes 27146-308 das Neves de Lima / AC	49009385
214	Gustavo	Moreira	ana-clara05@example.net	PF	2017-04-04 11:29:00-03	958.130.746-00	1968-01-25	Viela Manuela Rodrigues, 66 Alto Dos Pinheiros 62945-313 Oliveira / AL	54571-005
255	Julia	Moreira	silveiramelissa@example.net	PF	2022-10-10 07:25:00-03	396.270.158-30	1946-10-07	Estação Alexia Lopes, 5 Vila Da Amizade 51850-984 Rezende de Castro / PB	00262-103
277	Luiz Miguel	Moreira	martinsdiego@example.net	PF	2013-06-09 09:38:00-03	086.935.124-98	1987-12-09	Passarela de Santos, 98 Vila Barragem Santa Lúcia 30304-486 Melo de Oliveira / MG	67473094
438	Yasmin	Moreira	davi67@example.net	PF	2017-06-10 11:28:00-03	064.785.293-47	1981-06-22	Condomínio Correia, 742 São Salvador 32786903 Souza / SC	08955021
492	Davi Luiz	Moreira	oliviamelo@example.net	PF	2016-06-17 08:48:00-03	743.208.516-17	2002-12-14	Trecho Maysa Sales, 50 Vila Sesc 37960-340 Rezende / AL	87354941
513	Rafaela	Moreira	ada-paz@example.com	PF	2021-07-01 08:43:00-03	592.036.841-15	1968-08-31	Ladeira Pietra Carvalho, 8 Ouro Minas 44226-183 Araújo / PI	75408722
566	Cauã	Moreira	ana-clara57@example.org	PF	2013-11-11 10:53:00-02	025.138.946-42	1988-11-28	Viela Ana Cardoso Dona Clara 02078921 Vieira / AC	41520-936
583	Esther	Moreira	ibarbosa@example.net	PF	2020-06-25 11:02:00-03	793.401.682-40	1956-12-12	Trecho Kamilly Vieira, 25 Conjunto Floramar 63627215 Aragão / RO	99572275
677	Julia	Moreira	rezendethomas@example.net	PF	2018-01-15 09:14:00-02	825.316.940-05	1945-05-24	Pátio de Sales, 12 Juliana 79832367 Azevedo / RS	46015684
813	Ana	Moreira	tlima@example.org	PF	2020-08-28 08:27:00-03	952.306.178-02	1954-11-01	Via Miguel Pereira, 56 Boa Vista 90441-069 Alves do Norte / RS	16347-789
903	João Guilherme	Moreira	ypeixoto@example.com	PF	2020-08-16 08:05:00-03	721.086.459-85	1951-07-14	Trevo Novaes, 83 Vila Independencia 3ª Seção 63084022 da Conceição / PA	80883467
927	Catarina	Moreira	usales@example.com	PF	2011-08-15 11:48:00-03	356.841.972-64	1957-05-26	Vereda Nogueira, 258 Alta Tensão 1ª Seção 70468-406 Moura do Norte / AM	29576719
1	Vitor	Peixoto	ferreiradaniela@example.com	PF	2014-07-08 13:25:00-03	475.916.830-39	1993-09-27	Setor Mariane da Conceição, 21 São Luiz 78156-593 Pires de Silveira / GO	80160975
11	Esther	Peixoto	mnogueira@example.net	PF	2010-11-12 14:37:00-02	092.745.618-49	1975-11-22	Pátio Vitor Nogueira, 3 Vila Copacabana 34500762 Cardoso de da Costa / SE	60976-701
15	Francisco	Peixoto	gabriela62@example.net	PF	2022-09-11 12:51:00-03	451.732.069-34	1942-11-18	Praia Juan Moura, 27 Copacabana 41892762 Pinto da Serra / ES	58257-173
238	Leonardo	Peixoto	catarinaaraujo@example.net	PF	2019-03-08 13:08:00-03	231.680.457-80	1963-01-29	Recanto de Costa, 400 Ventosa 02897-918 Lopes do Galho / RR	54957-836
293	Milena	Peixoto	elisada-cunha@example.com	PF	2018-10-14 09:48:00-03	037.986.145-39	2007-02-12	Morro da Costa, 35 Miramar 55627-627 da Mata do Norte / SP	74310977
309	Pedro Lucas	Peixoto	pnascimento@example.org	PF	2015-07-15 08:21:00-03	281.049.673-04	1966-09-14	Avenida da Rocha, 3 Betânia 96725-893 Pires / PB	00803602
314	Luana	Peixoto	luiz-miguel20@example.com	PF	2017-02-11 10:07:00-02	158.390.764-57	2006-09-08	Campo Amanda Silveira, 306 Santo Antônio 73540760 Nascimento / RN	34847413
390	Amanda	Peixoto	leticiacostela@example.com	PF	2018-08-17 12:11:00-03	132.069.847-69	1992-10-27	Recanto Melissa das Neves, 1 Leonina 26637-569 Souza / AL	69658-168
637	Amanda	Peixoto	acunha@example.net	PF	2017-03-05 13:46:00-03	298.014.675-76	1986-11-22	Distrito Peixoto, 7 Senhor Dos Passos 56541895 Souza Paulista / SC	09777905
662	Cecília	Peixoto	da-cunhagabriela@example.net	PF	2022-12-15 09:50:00-03	539.864.120-42	1963-02-19	Estação Alves, 2 Estrela 93885-883 Ferreira / MA	58063933
748	Emilly	Peixoto	moreiraenzo@example.net	PF	2019-06-19 08:57:00-03	210.765.948-67	1991-07-25	Colônia Araújo, 967 Ápia 47383-578 Carvalho / ES	85405-606
784	Luiz Gustavo	Peixoto	peixotostella@example.com	PF	2020-07-27 09:34:00-03	482.036.715-35	1982-09-28	Condomínio de Silveira, 31 Prado 21560258 Novaes das Flores / AM	45895-886
797	Clarice	Peixoto	ylima@example.com	PF	2018-10-21 11:14:00-03	842.509.617-02	1989-07-06	Setor de Peixoto, 3 Camponesa 2ª Seção 37361701 Campos do Oeste / PR	32349-572
799	Giovanna	Peixoto	moraesjuliana@example.com	PF	2021-05-24 08:29:00-03	463.207.815-80	1964-10-04	Aeroporto Gonçalves, 15 Silveira 97640668 Rodrigues de Silva / AL	34250783
934	André	Peixoto	ramosisabelly@example.org	PF	2017-08-25 13:02:00-03	904.857.123-50	1957-12-13	Jardim Bruno da Mata Palmares 47723-078 Silva / RN	95211861
959	João Felipe	Peixoto	brendaoliveira@example.com	PF	2022-06-07 09:06:00-03	053.284.691-51	1943-01-10	Trecho Almeida, 13 Colégio Batista 24122732 da Paz / SC	87123267
969	Pietra	Peixoto	erickmoreira@example.net	PF	2020-10-18 07:41:00-03	398.402.715-04	1954-08-09	Vila de Lopes, 76 João Alfredo 45515876 Carvalho de Minas / RN	62717655
108	Maria Sophia	Pereira	limamarcelo@example.org	PF	2017-04-13 09:21:00-03	342.176.805-62	1950-12-01	Lago João Lucas Caldeira, 69 Campo Alegre 45988-418 Araújo / MT	97593-166
130	Juan	Pereira	maitepeixoto@example.com	PF	2019-11-15 07:31:00-03	034.912.687-96	1958-04-30	Lago Ian Duarte, 370 Santa Amelia 04743882 Lima Grande / CE	64577-629
164	Thiago	Pereira	ngoncalves@example.com	PF	2014-09-08 09:52:00-03	963.570.421-61	1978-02-17	Largo Camila Gomes, 181 Padre Eustáquio 42431193 Jesus / PA	18900-231
175	Joaquim	Pereira	gabriellysilveira@example.net	PF	2015-12-17 12:44:00-02	279.364.850-74	1956-08-11	Largo de Nascimento Santa Isabel 98609-522 da Cruz Verde / CE	19599-389
247	Stephany	Pereira	brenoda-cruz@example.com	PF	2019-07-25 11:05:00-03	957.482.106-49	1997-11-22	Aeroporto Lopes, 97 Alto Barroca 93134-328 Ferreira / AM	69958-930
302	Cauê	Pereira	ana-vitoria06@example.com	PF	2017-01-10 11:05:00-02	237.061.948-13	1972-08-25	Fazenda de Viana Minas Brasil 54626-764 Lima / SC	02056823
399	Ian	Pereira	nicoleda-rosa@example.com	PF	2018-09-23 13:32:00-03	158.329.740-50	1947-12-18	Condomínio Ana Lívia Aragão, 679 Betânia 46284612 Lopes / CE	77481-121
415	Thomas	Pereira	fariasbernardo@example.org	PF	2015-06-09 11:03:00-03	028.194.673-69	1994-08-11	Vereda de Campos, 31 Vila Sumaré 70152449 Vieira do Campo / SE	15155-764
420	Maria Cecília	Pereira	eduarda82@example.org	PF	2013-01-24 08:10:00-02	431.752.890-88	1999-07-19	Esplanada de Fernandes, 754 Vila Aeroporto 55518601 Ribeiro da Prata / AL	69843363
459	Calebe	Pereira	isabelly36@example.org	PF	2019-11-05 07:33:00-03	376.458.209-00	1955-02-17	Travessa Gabriel Fogaça São Cristóvão 29295-525 Freitas de Moura / MA	88159-092
570	Vinicius	Pereira	gomesgabriel@example.org	PF	2021-12-14 07:13:00-03	304.867.192-78	1991-04-06	Campo Maria Cecília Cunha, 30 Vista Do Sol 20399598 da Mota de Minas / AM	21811673
690	Murilo	Pereira	enovaes@example.org	PF	2022-02-27 11:01:00-03	248.963.570-00	1986-03-11	Viaduto de Moreira, 77 Vila Nova Gameleira 3ª Seção 36353-876 Silva / AC	58926-121
747	Sabrina	Pereira	iaraujo@example.org	PF	2022-11-18 11:04:00-03	589.734.210-50	1968-06-12	Colônia da Mota, 97 Vila Satélite 99485026 Pereira Alegre / BA	52359-242
806	Larissa	Pereira	gfernandes@example.com	PF	2018-05-02 10:32:00-03	721.083.496-69	1987-05-31	Estação Mariana da Luz, 415 Conjunto Jatoba 02240602 Teixeira / AM	88852254
820	Clara	Pereira	isadoramartins@example.com	PF	2017-05-04 09:48:00-03	013.967.824-78	1979-02-22	Rua Mendes, 275 Inconfidência 75773-173 Moraes / MS	42454829
905	Benício	Pereira	diegomartins@example.org	PF	2013-03-04 09:38:00-03	713.950.684-10	2005-09-03	Campo Sabrina Fernandes, 51 Aparecida 7ª Seção 82997893 da Rocha do Campo / AL	29896369
947	Mariana	Pereira	mfogaca@example.org	PF	2022-09-02 12:11:00-03	943.870.512-05	1980-11-19	Viaduto de Costa, 30 Das Industrias I 66789-389 Nogueira Alegre / PB	62135759
111	Calebe	Rezende	jvieira@example.org	PF	2021-11-02 08:34:00-03	391.276.054-34	1956-01-17	Estrada Ana Sophia da Cruz, 739 Marçola 46229-871 Monteiro / RO	04736-349
182	Vitor Gabriel	Rezende	fernandesbianca@example.com	PF	2020-11-04 09:06:00-03	468.903.721-31	1959-03-21	Trevo Ana Lívia Rocha, 65 Varzea Da Palma 54972834 Ramos / BA	12587-908
187	Maysa	Rezende	yfogaca@example.org	PF	2022-03-08 12:48:00-03	729.610.534-80	1964-05-16	Viela Novaes, 96 Vila Pilar 42684-816 Monteiro / RS	87469775
221	Sophie	Rezende	lauraalmeida@example.org	PF	2016-08-16 10:42:00-03	314.706.289-96	1990-05-28	Colônia Oliveira, 2 Outro 54229-031 Azevedo de Goiás / CE	96517-916
243	Danilo	Rezende	cgoncalves@example.com	PF	2017-04-01 11:23:00-03	043.159.687-57	1996-07-19	Trevo Nascimento Jardinópolis 99876-792 Ribeiro / AL	95216172
404	Ian	Rezende	heloisabarros@example.net	PF	2022-06-05 12:57:00-03	108.926.537-95	1971-05-04	Estrada de Lima, 19 Rio Branco 39661184 Aragão / TO	20897-575
482	Anthony	Rezende	jfernandes@example.net	PF	2019-08-23 08:22:00-03	740.935.612-06	1951-11-29	Trecho de Jesus, 5 Independência 70732-606 das Neves do Norte / DF	72954-765
529	Francisco	Rezende	clarice83@example.com	PF	2020-03-23 12:22:00-03	435.892.601-06	2002-02-16	Rua de Campos, 94 Salgado Filho 52999-749 Araújo / TO	46837-336
885	Sabrina	Rezende	juannogueira@example.org	PF	2018-03-01 12:45:00-03	260.438.975-47	1951-12-02	Viaduto das Neves, 379 Coração Eucarístico 96128-916 da Paz Alegre / DF	72048-406
41	Diego	Ribeiro	yuri36@example.org	PF	2020-02-28 09:44:00-03	061.278.594-76	1996-07-22	Área Alves Mineirão 21993173 Silveira / RN	18233-667
144	Lavínia	Ribeiro	maria-ceciliaviana@example.org	PF	2022-04-06 11:46:00-03	073.146.958-57	1968-07-31	Distrito Julia Nogueira, 72 Conjunto Capitão Eduardo 05771549 Jesus / AL	84431718
159	Isaac	Ribeiro	xfarias@example.com	PF	2022-06-24 10:01:00-03	524.108.367-71	1973-12-11	Sítio Farias, 47 Tirol 40205-749 Melo da Serra / RO	94207716
160	Maria Clara	Ribeiro	azevedobianca@example.net	PF	2019-11-02 13:31:00-03	634.178.509-75	1989-10-26	Lagoa de Sales, 67 Solar Do Barreiro 11113-671 Aragão de Viana / CE	68525237
203	Otávio	Ribeiro	piresmaria-vitoria@example.com	PF	2016-08-03 13:13:00-03	614.328.950-60	1980-12-21	Esplanada Silveira, 4 Fernão Dias 54477890 Freitas da Mata / PA	90460-313
222	Luna	Ribeiro	cavalcantijoaquim@example.com	PF	2014-05-11 07:03:00-03	643.572.108-44	1973-04-16	Esplanada de Costela, 42 Vila Mangueiras 62327599 Aragão / SP	06445-888
268	Heloísa	Ribeiro	vitor-gabriel19@example.net	PF	2016-07-06 13:25:00-03	502.867.493-00	1949-02-13	Residencial Lucas Gabriel Almeida, 634 Vista Alegre 56132889 Campos das Pedras / PB	20281879
282	Alice	Ribeiro	da-pazluiz-gustavo@example.net	PF	2017-10-03 09:41:00-03	316.458.920-42	1981-10-18	Lagoa de Oliveira Mirante 40346-315 Cunha / PB	08810-467
284	Isabelly	Ribeiro	davi12@example.org	PF	2020-02-18 10:51:00-03	523.648.709-92	1992-06-10	Área de Souza, 32 Vila Jardim Leblon 33602-241 Vieira / GO	00023-304
296	Caio	Ribeiro	paulo52@example.com	PF	2016-07-03 08:32:00-03	682.134.095-15	2001-11-24	Viaduto de Jesus, 23 Nossa Senhora Aparecida 16434898 da Mata do Sul / PR	17288-266
329	Mariana	Ribeiro	maria-fernanda87@example.org	PF	2019-06-25 12:18:00-03	149.682.035-51	1975-10-07	Feira Joaquim Duarte, 126 Serrano 37659718 Cavalcanti do Oeste / PB	07027312
405	Fernando	Ribeiro	falves@example.org	PF	2021-11-17 12:03:00-03	852.413.096-24	1972-07-11	Quadra João Vitor Nogueira, 132 Manacas 24012073 Nunes do Amparo / MA	61506780
430	Mariane	Ribeiro	marinaporto@example.org	PF	2013-08-16 08:02:00-03	625.847.193-55	1957-10-15	Rodovia de Alves, 200 Minaslandia 51402-995 da Cruz de Minas / MG	44708672
466	Yago	Ribeiro	nogueiraana-luiza@example.net	PF	2022-10-26 11:13:00-03	306.895.247-65	1975-11-18	Trevo Moreira, 283 Vila Madre Gertrudes 4ª Seção 51556-674 Correia / PI	98105-954
697	Joana	Ribeiro	juan52@example.net	PF	2013-01-24 10:46:00-02	652.943.780-00	1946-02-12	Vale Lima, 362 Fazendinha 56574753 Alves / MS	53155-194
803	Daniel	Ribeiro	joao80@example.net	PF	2021-11-17 09:01:00-03	408.697.531-93	2002-07-29	Esplanada Viana, 5 Caiçaras 45457828 da Cruz / SC	47382-055
861	Beatriz	Ribeiro	xjesus@example.org	PF	2018-07-15 08:31:00-03	250.946.183-42	2003-10-17	Trevo Ana Laura Rezende, 126 Caetano Furquim 81745943 Moreira / PE	06168-064
115	Kamilly	da Cruz	peixotoana-carolina@example.com	PF	2013-01-16 13:15:00-02	604.357.892-10	1952-07-31	Loteamento de Pereira, 58 João Pinheiro 31429-332 Araújo / PB	48067-920
123	Ana Laura	da Cruz	luccadas-neves@example.net	PF	2021-10-16 12:35:00-03	097.321.865-77	2003-05-09	Passarela Ferreira, 8 Guaratã 82155838 Silva / MA	22511466
344	Matheus	da Cruz	viniciusnogueira@example.org	PF	2022-05-25 10:35:00-03	537.648.290-10	1949-06-10	Lagoa Jesus, 867 Delta 68958-813 Pereira de Peixoto / PE	48430-435
351	Calebe	da Cruz	isabelda-cruz@example.com	PF	2020-08-19 13:52:00-03	340.981.267-96	1991-04-13	Estrada de Ramos, 663 Juliana 96893-650 da Luz / SP	67309619
440	Bruna	da Cruz	maria-juliaalmeida@example.org	PF	2022-03-02 08:10:00-03	213.075.698-03	1948-04-21	Quadra de da Paz, 57 São Tomaz 90868897 Nunes / MA	49195976
488	Juliana	da Cruz	lmoura@example.com	PF	2022-07-16 07:16:00-03	215.376.908-68	1966-04-16	Via Viana, 79 Vila Bandeirantes 39510-213 Peixoto de da Cunha / PE	09373098
531	João Guilherme	da Cruz	maria-alice81@example.com	PF	2020-04-17 10:12:00-03	270.365.981-40	1997-02-25	Estrada Isabel Gonçalves, 87 Vila Maria 70492743 da Luz do Norte / AC	62782-097
542	Sabrina	da Cruz	matheus57@example.com	PF	2022-06-02 08:54:00-03	240.538.769-74	1992-10-23	Vale João Pedro Caldeira, 64 Minas Brasil 73891-995 Alves / RJ	32727103
547	Gabrielly	da Cruz	mcardos0@example.net	PF	2013-04-04 12:11:00-03	671.489.023-04	1955-08-22	Núcleo de Costela, 58 João Alfredo 14810-102 Souza do Amparo / PI	69264-380
652	Fernanda	da Cruz	vda-rocha@example.net	PF	2020-10-23 12:23:00-03	036.419.278-03	1991-07-24	Largo Cecília Castro, 14 Alto Caiçaras 47705-021 Rocha / AL	69878-651
679	Sabrina	da Cruz	tramos@example.com	PF	2019-01-08 13:11:00-02	681.924.530-06	1949-10-09	Praça Leandro Pereira Satelite 36646-753 Gomes / PA	80537881
725	Elisa	da Cruz	barbaraalmeida@example.com	PF	2020-01-11 11:13:00-03	841.623.507-44	1976-03-12	Vila Maria Luiza Rezende, 18 Vila Jardim Alvorada 38369-203 da Paz / PI	60556-393
793	Rebeca	da Cruz	msouza@example.com	PF	2020-05-03 12:46:00-03	538.197.642-91	2002-06-19	Aeroporto de da Cunha Nova Cachoeirinha 33721-367 da Luz Alegre / RS	41051300
78	Thomas	da Mata	udias@example.net	PF	2019-12-27 09:52:00-03	603.457.918-00	1952-10-11	Via Rafaela da Rosa, 31 Castelo 42124264 Martins da Prata / SC	77149-544
136	Sarah	da Mata	maria-alice25@example.com	PF	2021-03-24 10:43:00-03	650.294.371-34	1970-08-23	Recanto João Miguel da Cruz, 40 Araguaia 02128-311 da Mota do Galho / AC	62165837
167	Sarah	da Mata	nsouza@example.org	PF	2021-08-21 11:10:00-03	096.187.354-00	1942-07-26	Condomínio de Ferreira, 60 Salgado Filho 72295750 da Paz / RJ	16431535
183	Júlia	da Mata	rda-paz@example.org	PF	2019-12-19 11:01:00-03	950.862.473-65	1948-06-04	Travessa Murilo Farias Castanheira 58338626 Freitas / GO	07445060
240	Henrique	da Mata	joao-lucas39@example.net	PF	2021-03-16 11:29:00-03	025.834.167-07	1977-06-29	Viaduto Ana Vitória Costa, 34 Vila Piratininga 74307-604 Santos / RS	93532415
310	Bernardo	da Mata	milena53@example.org	PF	2020-11-04 10:05:00-03	573.492.618-37	1952-02-08	Avenida Rafael Costa Nova Suíça 22991815 Cunha Verde / GO	05675-228
326	Lorenzo	da Mata	nascimentoigor@example.org	PF	2018-01-27 12:50:00-02	561.230.978-30	1964-05-01	Núcleo Theo Pinto, 91 Senhor Dos Passos 98613141 Freitas do Oeste / PB	40974811
340	Alexia	da Mata	isabel63@example.com	PF	2022-02-15 09:44:00-03	507.142.839-32	1993-11-29	Rua Amanda Novaes Vila São João Batista 88009706 da Rosa / SP	19736544
393	Alexia	da Mata	nunesluiz-felipe@example.net	PF	2022-10-27 13:27:00-03	567.294.380-29	1971-06-16	Fazenda de Araújo, 40 Nova Granada 80500197 Fogaça / CE	68814494
485	Vinicius	da Mata	melotheo@example.com	PF	2017-09-23 08:16:00-03	541.703.629-34	1944-11-27	Praça de Costa, 65 Vila Ipiranga 85905-432 Almeida / RO	86898-227
525	Amanda	da Mata	oliveiraolivia@example.com	PF	2012-05-03 10:10:00-03	607.891.234-87	2005-08-22	Esplanada de da Cruz Pilar 11944-945 Pereira / MA	45539156
546	Augusto	da Mata	kmoraes@example.org	PF	2019-12-14 13:56:00-03	409.361.582-98	2006-05-17	Setor Pires, 39 Minas Brasil 64587409 Pires do Amparo / AC	83642191
585	Luiz Fernando	da Mata	heloisa95@example.com	PF	2022-10-20 08:04:00-03	371.906.528-68	1991-09-22	Favela Manuela Pires, 80 Vila Minaslandia 95128146 Teixeira Paulista / AL	98547-972
618	Davi Lucas	da Mata	erick64@example.org	PF	2018-12-14 09:23:00-02	721.036.498-69	1958-10-11	Trevo Gustavo da Conceição, 54 Jardim Guanabara 62295901 Monteiro do Galho / DF	88416-256
742	Ana Sophia	da Mata	npinto@example.org	PF	2019-05-17 12:36:00-03	293.541.806-98	1976-08-13	Loteamento de da Rocha, 96 Santa Isabel 19282-292 Castro de Gomes / PI	63638-761
760	Lucca	da Mata	freitasmurilo@example.org	PF	2013-04-27 07:08:00-03	836.429.107-69	1983-09-21	Feira Alves, 65 Vila Petropolis 62254661 da Mota de Minas / MS	56414647
812	Heitor	da Mata	joao-pedrocunha@example.net	PF	2018-10-24 09:26:00-03	012.675.934-07	1978-06-23	Parque Ramos, 60 Olaria 10760399 Gomes de Barbosa / SP	70976-910
998	Alexandre	da Mata	ccunha@example.org	PF	2013-11-07 11:05:00-02	913.845.602-89	1945-10-26	Rodovia Alícia Silveira, 86 Pompéia 71107-614 Silva do Amparo / RS	16889847
47	Vinicius	da Mota	mendeseduarda@example.net	PF	2021-12-07 08:46:00-03	270.596.138-03	1984-01-20	Jardim Cavalcanti, 319 Santa Amelia 83518323 da Paz dos Dourados / MS	45833752
54	Luana	da Mota	mendesguilherme@example.com	PF	2017-09-08 09:55:00-03	478.925.013-04	2003-06-02	Sítio Maysa Nunes, 376 Ventosa 21912-741 Ferreira Paulista / SE	58810090
85	Pedro Henrique	da Mota	barrossabrina@example.org	PF	2010-10-04 10:19:00-03	256.847.910-85	1968-12-28	Fazenda de Almeida, 70 Conjunto Califórnia I 19254475 Rodrigues Alegre / SE	84785-656
138	Davi Lucas	da Mota	portovitor-gabriel@example.com	PF	2018-04-06 07:08:00-03	734.560.218-44	1964-11-08	Condomínio Ribeiro, 83 Santa Rita 43035840 Moreira do Oeste / ES	50910135
163	Luiz Gustavo	da Mota	julialopes@example.org	PF	2020-07-26 11:45:00-03	093.856.741-10	1955-09-06	Praça de da Cruz, 98 Tirol 76308996 da Costa de Oliveira / RN	29259296
198	Vitor	da Mota	castroerick@example.com	PF	2016-09-09 08:33:00-03	340.728.156-08	1968-11-16	Conjunto Cardoso, 45 Vila Bandeirantes 41654598 Duarte / DF	62913-346
389	Isabelly	da Mota	emillyalmeida@example.net	PF	2021-12-13 10:02:00-03	275.386.094-74	1961-03-27	Distrito Barbosa Maria Tereza 98433-264 Azevedo do Galho / PE	09722268
394	Joana	da Mota	silvadanilo@example.com	PF	2016-12-21 11:31:00-02	872.405.613-80	1969-07-09	Aeroporto de Nogueira, 90 Vila Piratininga Venda Nova 74845178 Rocha de Fogaça / GO	40557-484
464	João Felipe	da Mota	rdas-neves@example.com	PF	2021-04-20 07:09:00-03	156.729.083-30	1991-12-02	Conjunto de Costela, 452 Jardim Alvorada 25570466 Silveira de Pereira / SC	46903-963
518	Maria Sophia	da Mota	gabrielly74@example.net	PF	2015-10-03 11:31:00-03	456.029.378-38	1950-09-16	Largo de Correia, 42 Palmares 35281954 Mendes do Norte / CE	70049512
684	Beatriz	da Mota	vgoncalves@example.com	PF	2020-03-14 08:55:00-03	692.435.078-00	1987-07-12	Parque Teixeira, 91 Lourdes 39343662 das Neves do Galho / SP	90910-527
702	André	da Mota	juannogueira@example.com	PF	2017-05-25 12:10:00-03	876.104.953-01	1960-01-19	Esplanada Mariane Fogaça Boa União 2ª Seção 51644-388 Viana / CE	09413343
755	Giovanna	da Mota	pereiraraquel@example.net	PF	2019-05-27 10:47:00-03	872.456.019-76	1995-06-21	Feira da Paz, 25 Dona Clara 51727-260 Cunha da Serra / DF	55887339
777	Pietro	da Mota	maysa47@example.org	PF	2022-01-07 10:03:00-03	938.067.524-00	1948-03-06	Lagoa de Nunes, 343 Madri 72186-807 Rocha / MT	28353-018
816	Luna	da Mota	alexiaviana@example.net	PF	2017-04-18 12:01:00-03	371.690.485-66	1956-08-14	Viaduto de Ribeiro, 7 Ápia 14405-099 Ferreira / ES	38080-346
848	Maysa	da Mota	maria-julia26@example.org	PF	2022-10-14 11:02:00-03	068.317.429-04	1984-01-05	Estrada de Cardoso, 89 Novo Tupi 28144003 Novaes / MG	65236138
876	Enzo Gabriel	da Mota	ucastro@example.net	PF	2019-04-17 08:01:00-03	051.386.724-44	1984-05-11	Praça Araújo, 3 Campo Alegre 29924502 Gonçalves do Galho / BA	87757-995
913	Ana Clara	da Mota	cardosogabriela@example.net	PF	2021-10-08 11:19:00-03	061.742.598-11	1961-11-13	Fazenda Ana Júlia da Luz Mariano De Abreu 32644-259 Castro / PE	94039952
83	Eduardo	da Rosa	alexia28@example.com	PF	2021-10-07 10:04:00-03	452.837.109-04	1956-09-10	Quadra de Carvalho, 78 Vila Vista Alegre 70252-733 Costela / SP	61531-523
242	Pietra	da Rosa	joao-pedroda-luz@example.org	PF	2010-05-06 11:03:00-03	327.054.819-23	2001-03-21	Esplanada Fernandes, 59 São Marcos 32200-727 Costa / MT	63143-287
362	Stella	da Rosa	jpires@example.net	PF	2019-11-09 07:52:00-03	084.193.765-66	2001-03-09	Campo da Paz, 142 Vila Aeroporto 27339-168 Melo / AC	53746-671
424	Rodrigo	da Rosa	fribeiro@example.org	PF	2022-02-10 09:31:00-03	518.640.397-01	1950-02-17	Via Yago da Rocha, 460 Conjunto São Francisco De Assis 52195152 Gonçalves do Galho / RR	74607057
463	Lucas	da Rosa	da-cunhafernando@example.org	PF	2017-04-02 10:32:00-03	489.510.627-67	1958-05-28	Lagoa Maria Vitória Mendes, 579 São José 03752876 da Mata da Serra / AL	18035493
560	Leandro	da Rosa	joao-pedrooliveira@example.org	PF	2020-07-14 10:09:00-03	874.215.039-60	1967-12-21	Loteamento Matheus da Rocha, 3 Leticia 62507206 Monteiro / RS	59981945
628	Guilherme	da Rosa	vianalucas-gabriel@example.org	PF	2020-07-08 07:33:00-03	607.384.125-62	1955-02-08	Residencial Ferreira, 90 Vila São Dimas 05888019 Martins / AC	65356037
665	Amanda	da Rosa	stellacorreia@example.com	PF	2019-12-11 11:35:00-03	257.390.614-07	1995-01-01	Loteamento Lima, 1 Dom Bosco 24558786 Pereira de da Paz / SC	68868-878
703	Luiz Otávio	da Rosa	viniciusrodrigues@example.com	PF	2015-03-04 10:12:00-03	830.294.617-69	1972-01-07	Trecho de Lopes, 22 Estrela 39419543 Ribeiro Grande / RS	20114460
746	Emanuella	da Rosa	cpeixoto@example.net	PF	2021-08-27 12:36:00-03	348.672.519-09	1948-10-11	Praia Santos, 57 Marieta 3ª Seção 73009-630 Oliveira do Galho / RJ	60869553
770	João Pedro	da Rosa	das-neveskamilly@example.net	PF	2022-04-04 09:34:00-03	265.047.391-61	2006-06-06	Favela Azevedo, 11 Conjunto Taquaril 40563793 Viana / SE	21981021
895	Joana	da Rosa	valentinamartins@example.net	PF	2022-08-15 08:35:00-03	274.516.890-85	1963-07-21	Residencial Guilherme da Luz, 9 Mangueiras 64912-680 Ramos / AP	11658-609
907	Miguel	da Rosa	ggomes@example.org	PF	2022-08-01 09:44:00-03	315.649.280-98	1962-07-02	Viaduto de Novaes, 61 Vila Maloca 51271851 Farias / PR	77289-010
31	Sophia	Caldeira	silveirarodrigo@example.org	PF	2016-09-25 11:39:00-03	196.427.385-46	1977-03-05	Condomínio de Cunha, 30 Vila Paquetá 05930604 Barros da Serra / MS	60759944
49	Juliana	Caldeira	rpeixoto@example.net	PF	2021-05-05 09:58:00-03	457.380.619-93	1977-09-28	Recanto de Nogueira, 4 Boa Vista 12270-586 Souza / ES	44667-028
88	Maysa	Caldeira	rafael21@example.net	PF	2020-10-07 11:15:00-03	702.614.935-07	1999-10-14	Ladeira Bianca Gomes, 7 Ipiranga 71245492 Pires de Almeida / RN	26042-921
132	Leonardo	Caldeira	eduarda83@example.net	PF	2022-08-18 08:53:00-03	316.047.289-22	1945-10-12	Distrito de Silva, 709 Granja De Freitas 49834-233 Farias de Farias / SP	53952-163
207	Francisco	Caldeira	gabriellyfreitas@example.com	PF	2022-06-24 13:07:00-03	946.517.283-91	1954-05-18	Distrito Rebeca Fogaça, 78 Savassi 67766103 Almeida de Goiás / RJ	26236980
249	Marcela	Caldeira	mariane80@example.com	PF	2019-01-10 12:25:00-02	140.687.293-87	1956-07-10	Sítio Vitor Hugo Castro, 7 Vista Do Sol 32863094 Cunha / MA	20163-136
608	Isabelly	Caldeira	voliveira@example.net	PF	2014-09-20 08:49:00-03	347.092.185-79	1978-03-26	Feira de Alves, 51 Marieta 1ª Seção 26624-408 Barros do Galho / SE	53011234
701	Milena	Caldeira	diogo43@example.net	PF	2013-02-24 07:46:00-03	965.041.728-11	1966-08-24	Feira Dias, 3 São Paulo 25699-190 Lima / MA	19025-885
817	Kamilly	Caldeira	pmonteiro@example.com	PF	2021-11-25 12:41:00-03	504.217.836-80	1947-05-25	Favela Porto, 58 Vila Satélite 55942548 Monteiro / TO	14508-865
823	Levi	Caldeira	yagoramos@example.com	PF	2022-01-18 09:45:00-03	092.548.671-01	1968-12-17	Praia Maria Cecília Cardoso, 883 Vila Havaí 80650052 Carvalho / RR	54165313
863	Emanuel	Caldeira	pedro54@example.net	PF	2015-11-26 10:41:00-02	176.584.029-58	1977-10-19	Rua Maria Clara Barbosa, 30 Petropolis 87686393 da Paz de Ramos / BA	30690903
881	Luiz Gustavo	Caldeira	hviana@example.org	PF	2019-12-23 12:28:00-03	859.762.130-30	1957-03-07	Aeroporto Oliveira, 888 Dom Cabral 50785807 Rodrigues / TO	58956-583
926	Luiz Miguel	Caldeira	lopeshelena@example.net	PF	2018-05-03 12:18:00-03	274.816.539-09	1950-07-22	Ladeira de Silveira, 85 Vila Jardim Montanhes 92682195 Pinto da Prata / AL	42798399
976	Vicente	Caldeira	da-mataluiza@example.net	PF	2015-09-16 12:49:00-03	761.382.059-95	1963-06-05	Vereda Almeida, 1 Vila Santo Antônio Barroquinha 47821-847 Rocha / CE	25244388
287	Letícia	Carvalho	lunasales@example.org	PF	2020-07-20 10:04:00-03	938.526.017-03	1944-04-03	Vereda Silva, 55 Andiroba 83971874 Fernandes Verde / RN	07567278
324	Diego	Carvalho	rochamaria-clara@example.org	PF	2022-01-11 12:19:00-03	417.895.230-79	1989-02-11	Campo de Barbosa, 5 Itatiaia 37893-563 Silva Grande / AP	64342245
376	Henrique	Carvalho	valentina99@example.com	PF	2012-04-07 12:08:00-03	843.527.109-97	2001-11-22	Trevo Peixoto Coqueiros 30372098 Moura do Norte / PI	84075350
423	Heloísa	Carvalho	enzogoncalves@example.net	PF	2017-11-26 14:12:00-02	512.748.639-37	1992-10-10	Distrito Jesus, 3 Santa Maria 49567-381 da Rosa / RN	99953812
448	Lucas Gabriel	Carvalho	maria-sophia95@example.org	PF	2019-12-18 10:11:00-03	713.028.965-12	1996-03-27	Avenida Nicolas Farias, 138 Tupi A 73590-026 Rezende / SC	19905255
503	Emanuel	Carvalho	liviaviana@example.com	PF	2020-11-25 10:30:00-03	234.159.780-79	1969-01-27	Conjunto Ramos São Sebastião 39944-896 Sales da Prata / PE	11832-547
617	Melissa	Carvalho	almeidacatarina@example.com	PF	2016-02-17 09:37:00-02	327.548.609-83	1973-04-03	Recanto Caldeira, 2 Boa Viagem 86510311 Gomes Paulista / CE	26853-361
646	Caroline	Carvalho	thalesrodrigues@example.net	PF	2018-08-15 12:30:00-03	286.175.943-73	1962-04-21	Pátio Thomas Novaes, 21 Vila Aeroporto Jaraguá 86235684 Rezende / AC	64172-961
724	Bryan	Carvalho	rporto@example.com	PF	2019-08-07 10:31:00-03	847.395.126-37	1984-06-12	Área Elisa Mendes, 60 Jaqueline 78185518 Almeida do Sul / RJ	76440-267
831	Catarina	Carvalho	dcarvalho@example.org	PF	2016-01-22 09:28:00-02	401.796.528-85	1974-04-25	Colônia de Castro, 30 Belvedere 62950-196 Rodrigues / PB	63295-939
872	Breno	Carvalho	bfernandes@example.org	PF	2021-01-16 07:38:00-03	120.897.653-21	1967-05-26	Rua Marina Gomes Esperança 60145-320 Vieira / ES	94339-803
64	Agatha	Ferreira	waraujo@example.com	PF	2016-04-18 12:57:00-03	309.714.582-60	1958-05-29	Trevo Freitas, 66 Vila Petropolis 19175-991 Cunha / PA	07428521
248	Clarice	Ferreira	enrico46@example.org	PF	2019-06-13 08:34:00-03	612.895.734-09	1958-01-31	Largo Thales Farias, 64 Conjunto Jardim Filadélfia 16733591 Moreira / CE	93633-706
447	Emanuel	Ferreira	laisnascimento@example.com	PF	2022-03-08 12:28:00-03	207.168.394-31	1972-09-27	Vale de Duarte, 682 Lourdes 03846-664 Rezende do Galho / RO	97675-667
599	André	Ferreira	ana-sophiada-costa@example.org	PF	2020-01-11 11:52:00-03	087.294.653-38	2004-04-13	Fazenda Gustavo Ferreira, 6 Alto Caiçaras 40375687 Nascimento de Monteiro / GO	97685445
644	Luigi	Ferreira	dda-conceicao@example.com	PF	2022-11-06 09:02:00-03	967.835.140-48	1992-03-08	Rodovia de Freitas, 44 Europa 23099-905 Carvalho Alegre / TO	12155532
699	Pedro Miguel	Ferreira	ksilva@example.org	PF	2011-03-22 11:49:00-03	941.720.856-01	1989-10-13	Estrada Dias, 170 Alta Tensão 1ª Seção 63660-116 Ramos Alegre / CE	89280-864
791	Gustavo	Ferreira	ida-costa@example.net	PF	2015-10-21 11:49:00-02	460.751.283-44	1945-11-18	Praça Aragão, 47 Novo Das Industrias 92998996 Mendes / ES	68092-313
828	Sophia	Ferreira	caldeirabryan@example.org	PF	2021-12-25 09:52:00-03	856.124.073-35	1981-12-03	Travessa de Ferreira, 6 Aarão Reis 77221-591 da Paz da Praia / RO	40467-614
984	Gustavo Henrique	Ferreira	sophiearaujo@example.net	PF	2016-12-15 10:04:00-02	132.604.978-03	1946-11-01	Vale Lorena Teixeira, 80 Cidade Jardim Taquaril 43753518 Cardoso do Galho / RR	81193-115
996	Vicente	Ferreira	maria-vitorianunes@example.org	PF	2017-02-20 11:29:00-03	235.097.618-12	1997-04-19	Trecho Sales Granja Werneck 29929103 Silveira do Galho / SE	86453320
179	Thiago	Monteiro	silvasophia@example.net	PF	2019-01-15 09:29:00-02	029.674.851-01	1952-02-28	Trecho de Farias, 906 Manacas 66718-074 Moraes / AP	58661-890
338	Erick	Monteiro	carlos-eduardo61@example.com	PF	2016-03-07 11:26:00-03	649.085.213-60	1967-01-03	Viela de da Rosa, 86 Santa Amelia 78590-222 Vieira / RR	59633-454
357	Evelyn	Monteiro	nicolearaujo@example.com	PF	2021-07-12 12:17:00-03	648.395.702-56	1951-10-29	Rua Correia, 7 Marilandia 57967380 Porto do Oeste / MA	87506-304
386	Nina	Monteiro	meloana-sophia@example.net	PF	2018-11-03 09:58:00-03	739.146.082-69	1987-10-04	Quadra Gomes Vila Piratininga Venda Nova 39354-670 Silva / SC	60999280
409	Juliana	Monteiro	ryanrezende@example.net	PF	2015-11-08 12:10:00-02	035.916.824-89	1999-03-03	Largo de Farias, 8 Santa Sofia 78450-908 Fogaça / RR	55913-053
562	Mirella	Monteiro	isabelsouza@example.net	PF	2021-07-16 13:50:00-03	342.971.860-04	1959-03-18	Distrito de Cavalcanti, 14 Horto Florestal 55285195 Moraes / ES	90139-977
565	Enzo	Monteiro	uduarte@example.net	PF	2016-06-28 09:31:00-03	507.849.231-32	1948-09-28	Passarela de Monteiro, 25 Vila Antena 50344904 Almeida / TO	12160847
630	Thiago	Monteiro	yago29@example.com	PF	2014-07-02 09:56:00-03	143.295.670-16	1945-07-25	Passarela de Almeida, 279 Etelvina Carneiro 80838683 Costela das Flores / DF	87901-288
660	Fernando	Monteiro	natalia51@example.net	PF	2022-10-28 11:34:00-03	298.137.645-46	1965-10-27	Conjunto Moraes, 6 Vila Petropolis 93440257 Costela / SC	78398-688
734	Luna	Monteiro	stella79@example.org	PF	2020-08-17 12:00:00-03	786.152.093-59	2001-10-22	Área Clara Castro, 91 Serra 90010378 Duarte Grande / MG	89964-963
865	Danilo	Monteiro	leticia65@example.org	PF	2019-05-06 07:03:00-03	813.469.705-48	1981-07-27	Área Bruna Pinto, 9 Tres Marias 69314-474 Costela / PI	95694-426
962	Marina	Monteiro	joao-lucasoliveira@example.org	PF	2022-07-19 12:11:00-03	712.435.069-70	1984-01-16	Feira da Conceição Vila Do Pombal 25982975 Ramos / BA	01613203
23	Sabrina	Nogueira	lara92@example.net	PF	2012-12-06 08:38:00-02	105.362.874-90	1987-04-12	Recanto de Viana, 51 Cinquentenário 35460-305 Mendes / PB	45240598
156	Matheus	Nogueira	leticiacostela@example.org	PF	2018-09-03 12:21:00-03	205.649.318-70	1969-08-08	Rodovia de Nogueira, 79 Solimoes 99124-289 Fernandes Grande / RS	46469073
470	Heloísa	Nogueira	juanteixeira@example.com	PF	2016-10-03 11:01:00-03	290.415.738-79	1955-02-24	Largo Enrico Moreira, 82 Nossa Senhora Do Rosário 07570669 da Mata / RS	27276817
508	Catarina	Nogueira	brendaalmeida@example.net	PF	2019-05-18 11:07:00-03	269.031.847-40	1962-02-17	Setor de Rezende, 1 Maria Tereza 91022434 Gonçalves / SC	37113-152
509	Matheus	Nogueira	camposmaria-clara@example.org	PF	2012-01-20 09:56:00-02	507.689.421-01	1962-11-15	Campo Nunes, 35 Calafate 93091-401 Moura Grande / RS	29657-764
533	Alana	Nogueira	mcastro@example.com	PF	2021-08-09 09:39:00-03	423.518.970-05	1977-08-22	Trevo Benjamin Dias, 33 Vila Independencia 1ª Seção 46747249 Barbosa Alegre / RN	21601596
634	Paulo	Nogueira	pedro-lucas85@example.com	PF	2021-07-02 10:31:00-03	758.349.012-04	2002-07-16	Vila Cardoso, 77 Leticia 51050-423 Mendes da Praia / BA	80483580
668	Maria Julia	Nogueira	gramos@example.net	PF	2015-03-03 09:16:00-03	305.187.462-08	1957-02-10	Lagoa Carvalho, 4 Jardim Felicidade 92014053 Ramos Verde / PB	29165027
757	Julia	Nogueira	luigi30@example.com	PF	2016-12-10 13:46:00-02	471.690.358-39	1986-08-06	Vereda de Correia, 1 Marieta 3ª Seção 91998-530 Monteiro / RR	79522419
774	Valentina	Nogueira	eduardo99@example.net	PF	2018-07-11 11:52:00-03	726.435.910-70	1978-12-18	Passarela de da Rosa, 17 Cardoso 56527391 Rezende do Galho / PI	59960-080
839	Clarice	Nogueira	pintocaio@example.org	PF	2017-01-02 10:37:00-02	182.905.463-51	1970-05-13	Colônia de da Mata, 7 Xangri-Lá 52109327 Campos das Flores / MG	34927-035
889	Bianca	Nogueira	moraesnicolas@example.org	PF	2022-11-20 11:49:00-03	169.502.874-02	2002-01-03	Viaduto Lorena da Rocha, 31 Vila Calafate 57952-050 Peixoto de Campos / MA	69613-287
995	Vitor Hugo	Nogueira	castrovitoria@example.org	PF	2021-02-06 10:22:00-03	413.702.968-78	1987-01-11	Vila Maria Cecília da Luz, 90 Santa Isabel 65394776 da Conceição / SP	05860602
69	Ana Júlia	Oliveira	isis11@example.net	PF	2019-10-26 11:52:00-03	210.643.578-90	1985-08-29	Viela Gonçalves, 524 São Gabriel 85020155 da Paz de Farias / MS	48276-010
96	Bryan	Oliveira	rezendejulia@example.org	PF	2022-08-15 10:30:00-03	163.250.789-77	1979-02-19	Passarela Oliveira, 453 Conjunto Jardim Filadélfia 18924578 Pinto / SP	99252052
125	Cauê	Oliveira	thiago40@example.org	PF	2019-04-08 09:41:00-03	538.072.694-10	1985-09-05	Chácara de Gomes, 660 Ernesto Nascimento 99378-173 Lopes Alegre / RR	61566532
154	Ana Júlia	Oliveira	kevin24@example.org	PF	2013-10-14 08:46:00-03	059.187.432-60	1950-05-30	Lagoa de Araújo, 41 Venda Nova 32097449 Silva / RN	51218-272
180	João Gabriel	Oliveira	cmonteiro@example.org	PF	2022-07-21 09:36:00-03	925.138.746-09	1995-03-16	Núcleo da Luz, 80 Vila Sumaré 45305-398 da Rocha / SE	42069-597
196	Vitória	Oliveira	moraescaio@example.org	PF	2014-06-05 09:16:00-03	196.378.240-22	1978-07-11	Viela Joana Duarte, 96 Tupi B 41685-145 Pinto / RO	10703-141
199	Murilo	Oliveira	caldeirayuri@example.com	PF	2019-01-10 10:24:00-02	061.984.573-20	1943-08-12	Sítio de da Conceição, 21 Suzana 63330113 Pires / MS	59066-697
256	Agatha	Oliveira	monteirosofia@example.net	PF	2020-04-28 13:46:00-03	896.130.725-86	1982-01-26	Recanto Bernardo Rezende, 11 São Damião 25575-266 Caldeira / SP	50987-872
271	Larissa	Oliveira	da-luzmarcos-vinicius@example.com	PF	2020-12-02 09:41:00-03	764.092.138-40	2000-02-04	Praia de Mendes, 99 São Paulo 59552-991 Novaes / AC	93866-463
273	Vitor Gabriel	Oliveira	osilveira@example.net	PF	2020-04-27 08:42:00-03	403.172.598-14	1969-07-08	Avenida Caldeira Vila Oeste 31522-748 Alves do Campo / ES	67970-617
301	Stephany	Oliveira	maria-sophianunes@example.org	PF	2015-10-20 11:23:00-02	329.571.680-30	1945-01-22	Ladeira Barbosa, 905 Delta 83154-332 Oliveira / MS	44209-089
315	Lívia	Oliveira	ramospietra@example.net	PF	2022-12-19 11:37:00-03	527.806.349-10	1980-06-30	Estrada de Carvalho, 10 Jardinópolis 16471842 Caldeira do Campo / AC	39563-033
519	Bryan	Oliveira	kda-luz@example.org	PF	2021-09-14 08:54:00-03	073.218.694-31	1952-06-11	Aeroporto Jesus, 59 Vila Madre Gertrudes 2ª Seção 55659-448 Martins das Flores / AL	10142341
581	Enrico	Oliveira	castroheitor@example.net	PF	2017-07-06 08:09:00-03	278.906.415-67	1967-07-20	Alameda Renan da Cruz, 76 Vila Nova Cachoeirinha 3ª Seção 83487207 da Costa da Serra / SP	15723-507
688	Alexandre	Oliveira	vicenteda-conceicao@example.net	PF	2017-12-05 09:03:00-02	560.941.382-60	1987-05-22	Morro Otávio Costela, 18 Vila Suzana Primeira Seção 79865617 Nunes Verde / RS	32672164
715	Bruno	Oliveira	calebe23@example.net	PF	2018-12-20 14:17:00-02	912.685.734-00	1973-09-07	Parque de Duarte, 58 Vila Esplanada 21284-209 Dias / AP	39393-555
451	Stella	Silveira	esther63@example.org	PF	2011-08-25 08:33:00-03	583.260.971-95	2000-01-03	Viela Sophia Peixoto, 95 Andiroba 01362-364 Rocha / MT	44492-634
472	Matheus	Silveira	da-cunhaluiz-miguel@example.org	PF	2022-08-07 09:08:00-03	608.573.421-26	1981-03-12	Rua Maria Julia da Costa, 3 Penha 14678-858 Pinto do Amparo / BA	96254453
511	Murilo	Silveira	da-costabarbara@example.org	PF	2018-11-07 09:57:00-02	317.406.829-04	1964-12-27	Vale Vinicius da Cunha, 6 Vila Santo Antônio Barroquinha 48223-320 Gonçalves / ES	71158-691
526	Rafaela	Silveira	joao-miguel21@example.net	PF	2018-09-16 12:21:00-03	178.936.250-40	1982-03-03	Avenida Yuri Santos, 56 Boa União 1ª Seção 28672289 da Cunha / SC	71513521
539	Benjamin	Silveira	lda-mata@example.com	PF	2011-04-21 07:58:00-03	983.416.270-78	1960-06-05	Chácara Campos, 85 Boa União 1ª Seção 87856223 Rocha do Campo / BA	02025883
549	Mirella	Silveira	joanasouza@example.com	PF	2019-06-22 12:37:00-03	290.167.845-94	1990-08-01	Praia Gomes, 99 Buritis 00506278 Fogaça do Oeste / MS	25765-264
666	Lucca	Silveira	barbara78@example.com	PF	2015-07-27 12:03:00-03	195.842.037-97	1947-11-08	Feira Silva, 1 Monsenhor Messias 47727-688 Barbosa / RR	23978-780
751	Maria Sophia	Silveira	mda-luz@example.com	PF	2012-01-13 10:29:00-02	189.647.205-20	1989-05-25	Núcleo Nina Castro Universo 61010121 Ribeiro / RS	62560-172
782	Erick	Silveira	rebeca66@example.com	PF	2019-01-10 09:19:00-02	691.837.524-55	1952-05-23	Praia Pedro Lucas Porto, 92 Maria Goretti 68997814 Caldeira / MT	52820470
925	Noah	Silveira	ana-beatriz52@example.com	PF	2014-07-10 11:52:00-03	278.410.935-60	2001-09-29	Pátio de Melo, 929 Padre Eustáquio 71368151 Moura / AM	90467127
949	Samuel	Silveira	catarinaporto@example.com	PF	2016-07-07 07:30:00-03	473.820.196-40	1986-10-20	Esplanada Moraes, 202 Vila Nova Gameleira 3ª Seção 90602-498 Monteiro da Mata / TO	34143-462
954	Augusto	Silveira	ana-clara17@example.net	PF	2012-04-15 07:23:00-03	459.762.083-47	1983-01-25	Recanto Souza, 43 São Luiz 36309550 Aragão / ES	86541-029
956	Mariane	Silveira	vieiralevi@example.net	PF	2017-07-11 12:45:00-03	647.528.019-49	1988-03-12	Quadra de Araújo, 10 São Francisco Das Chagas 39063132 Barbosa de Minas / RS	32056-771
975	Levi	Silveira	aalves@example.com	PF	2021-03-18 11:58:00-03	560.824.173-80	1974-10-04	Ladeira de Viana Vila Santa Monica 2ª Seção 59516234 Costela / MG	17928-914
22	Olivia	Teixeira	hbarros@example.com	PF	2020-11-14 08:11:00-03	415.879.203-79	1946-09-19	Fazenda Vinicius Rocha, 39 Cinquentenário 88559026 da Luz / PE	23793323
98	Letícia	Teixeira	alana90@example.net	PF	2018-02-19 08:29:00-03	506.187.924-49	1955-09-18	Chácara de das Neves, 55 Alto Caiçaras 74632702 da Cunha / RJ	15033744
202	Luigi	Teixeira	goncalveskevin@example.com	PF	2016-04-22 08:19:00-03	675.204.138-53	1963-01-01	Largo da Costa, 811 Madri 34204001 Ferreira Alegre / PA	29819-675
260	Alana	Teixeira	cavalcantimarcos-vinicius@example.com	PF	2013-10-22 12:33:00-02	432.150.768-53	1992-07-09	Aeroporto Lopes, 1 Vila Jardim Leblon 32361498 Oliveira / SP	83270255
278	Alexia	Teixeira	lunapereira@example.org	PF	2020-04-17 12:47:00-03	670.391.825-21	1978-10-18	Residencial Gonçalves, 273 Das Industrias I 53878542 Jesus / PI	46177-965
305	Raul	Teixeira	catarinacunha@example.com	PF	2021-07-13 10:43:00-03	136.897.240-31	1944-04-10	Vale Martins, 83 Jardim Dos Comerciarios 86343-612 Nogueira / MT	00264638
308	Enrico	Teixeira	juan30@example.net	PF	2018-06-26 11:23:00-03	415.920.836-33	1959-07-12	Quadra Anthony Viana, 65 Vista Do Sol 06902497 Lima / PI	13700963
349	Catarina	Teixeira	da-costaalana@example.com	PF	2019-01-07 13:47:00-02	375.492.086-38	1966-12-16	Viaduto de Fogaça, 5 Andiroba 60168-433 Correia de Minas / MS	80534-607
401	Murilo	Teixeira	eloah85@example.org	PF	2020-12-18 08:53:00-03	329.608.514-98	1965-02-22	Estação de Martins, 42 Dom Joaquim 32620783 Vieira / MG	38783875
444	Bernardo	Teixeira	da-pazcamila@example.org	PF	2022-11-20 13:56:00-03	036.827.954-56	1954-10-12	Área Cunha, 26 Marmiteiros 63250034 Nunes Verde / AM	40294-301
460	Matheus	Teixeira	freitasagatha@example.org	PF	2014-03-04 07:43:00-03	201.679.843-22	1991-08-03	Passarela Laura Lima, 662 Colégio Batista 92477-191 Nunes / RO	21837993
561	Felipe	Teixeira	oliveirafernanda@example.net	PF	2019-01-20 10:19:00-02	728.649.031-13	1977-05-30	Sítio da Luz, 56 Vila Independencia 1ª Seção 51453-241 Caldeira da Mata / MG	83855198
603	Francisco	Teixeira	fogacahenrique@example.net	PF	2018-04-20 07:44:00-03	983.702.416-03	1966-04-29	Fazenda Rafael Ramos, 79 Guarani 21276-120 Nascimento / DF	80604493
613	Isabelly	Teixeira	thalesviana@example.com	PF	2017-10-24 11:13:00-02	846.751.392-64	1952-03-21	Largo da Luz, 740 Conjunto Jardim Filadélfia 63173687 Azevedo dos Dourados / SC	69346548
632	Isadora	Teixeira	mcunha@example.org	PF	2017-07-25 13:41:00-03	951.786.204-02	1986-02-26	Alameda Moura, 33 Bonsucesso 32984-617 Farias / CE	51467-373
761	Murilo	Teixeira	stephanyda-costa@example.net	PF	2016-06-07 12:04:00-03	874.125.903-32	1992-01-15	Viela Felipe Rodrigues, 85 Nazare 16000115 Silva / PB	06896584
764	Camila	Teixeira	ana46@example.org	PF	2022-09-08 08:06:00-03	304.867.925-10	1961-10-30	Fazenda de Dias, 1 Marajó 72368-137 da Conceição / CE	92971-218
951	Matheus	Teixeira	helenaalmeida@example.org	PF	2018-11-19 09:53:00-02	065.184.729-01	1955-07-21	Trecho Viana, 7 Corumbiara 51229-101 da Costa / MG	20530-830
12	Brenda	da Costa	azevedojulia@example.net	PF	2016-01-13 10:28:00-02	519.837.046-01	1984-02-11	Viela Gonçalves, 56 Silveira 83067537 Cardoso do Norte / AL	40899-331
24	Carlos Eduardo	da Costa	augusto70@example.org	PF	2018-06-10 08:18:00-03	093.528.416-89	1951-07-21	Distrito Theo Lima, 75 Mineirão 64887091 Nogueira da Serra / AC	98215323
34	Gustavo	da Costa	aaraujo@example.com	PF	2022-08-02 12:03:00-03	532.741.806-53	1970-01-20	Praça Luigi Ribeiro Mineirão 63769144 Viana do Sul / RN	34095-618
46	Vicente	da Costa	hferreira@example.net	PF	2011-06-04 08:10:00-03	436.859.201-89	2005-12-22	Largo Rodrigues Bela Vitoria 65601-692 Araújo / AM	30446-154
80	Ana Laura	da Costa	ffernandes@example.com	PF	2015-04-23 12:08:00-03	830.246.957-29	1963-03-07	Distrito Campos, 13 Alto Vera Cruz 71980-279 Ferreira / AP	52750-511
121	Igor	da Costa	souzaana-vitoria@example.org	PF	2021-02-08 08:22:00-03	521.468.390-15	1990-11-03	Avenida Ian Fernandes, 58 Bandeirantes 90717-478 Rocha de Minas / RS	36757-275
257	Emanuel	da Costa	zcorreia@example.com	PF	2021-05-20 11:11:00-03	305.864.291-15	1945-05-14	Trevo Ana Julia Castro, 3 Alto Barroca 95616-181 da Cunha do Galho / TO	26342-184
458	Catarina	da Costa	cardosodavi-lucca@example.net	PF	2017-07-02 11:47:00-03	593.701.482-05	2001-03-08	Residencial Matheus Melo, 546 Braúnas 87347690 Pires de Peixoto / SE	57749312
572	Fernando	da Costa	eloah44@example.com	PF	2021-09-21 13:34:00-03	859.712.406-76	1961-12-22	Travessa Elisa Lopes, 37 Vila Nova Gameleira 1ª Seção 80920585 da Paz / MT	57985897
578	Pedro Miguel	da Costa	matheus28@example.com	PF	2021-02-03 11:52:00-03	367.910.845-10	1957-12-12	Praia Peixoto Lagoa 75181268 Costa / RR	38041-732
673	Davi Lucca	da Costa	rnovaes@example.org	PF	2016-09-09 11:30:00-03	580.961.732-86	1946-07-20	Rua Pires, 88 Betânia 01728829 Correia do Galho / DF	87335481
682	João Lucas	da Costa	da-rosacarlos-eduardo@example.org	PF	2020-10-14 13:53:00-03	429.756.308-83	1950-06-17	Aeroporto Lorena Souza, 5 Santa Isabel 27905-727 Viana Paulista / PR	12373200
713	Felipe	da Costa	clariceda-cruz@example.com	PF	2019-10-10 07:50:00-03	794.385.602-38	1953-02-03	Fazenda de Caldeira, 46 Outro 76868-632 Vieira de Minas / SP	98383701
739	Bárbara	da Costa	vianaheloisa@example.org	PF	2014-02-26 09:20:00-03	065.189.437-93	1959-09-04	Lago Nathan da Mota São Damião 56497734 Teixeira / CE	11452-459
768	Anthony	da Costa	wcarvalho@example.net	PF	2019-04-22 11:34:00-03	305.692.847-83	1981-09-30	Sítio da Mata, 75 Vila Boa Vista 66793306 da Rosa / ES	49032-305
826	Breno	da Costa	luna46@example.net	PF	2017-07-16 07:31:00-03	498.367.052-10	1943-03-23	Avenida de Pires, 62 Vila De Sá 86955148 Silveira da Serra / BA	84921499
841	Ana Beatriz	da Costa	otavioalves@example.com	PF	2020-04-16 13:11:00-03	562.709.431-16	2006-03-30	Passarela Vieira, 71 Caiçaras 67045387 Campos / AC	14362606
904	Diogo	da Costa	beniciosilva@example.net	PF	2020-12-16 11:36:00-03	486.217.350-08	1986-01-19	Trecho de Monteiro, 46 Garças 77378059 Azevedo de Cardoso / AC	09304-471
967	Guilherme	da Costa	barrosmelissa@example.net	PF	2016-02-21 11:33:00-03	027.531.946-61	1973-05-31	Conjunto Calebe Dias, 30 Vila Boa Vista 29952-706 Alves da Serra / MS	33421319
25	Augusto	da Cunha	melissacardoso@example.com	PF	2022-09-04 07:40:00-03	172.460.893-22	1944-02-29	Morro Gonçalves, 47 Santa Sofia 26547-604 Vieira do Amparo / AL	07616-570
44	João Pedro	da Cunha	ybarros@example.com	PF	2020-09-07 11:32:00-03	835.147.906-39	2003-06-23	Área Luigi Rodrigues, 458 Aarão Reis 02379318 Gomes Alegre / PR	25857675
91	João Miguel	da Cunha	hda-paz@example.com	PF	2020-02-17 07:45:00-03	912.740.638-50	2003-03-03	Aeroporto Isabella Oliveira, 61 Jaqueline 14925-009 Alves das Pedras / ES	97402-978
104	Heloísa	da Cunha	barbosayasmin@example.net	PF	2018-01-04 10:09:00-02	921.764.538-91	1943-08-01	Travessa Sarah Aragão, 19 Marilandia 51955-163 Fernandes Grande / DF	92815998
161	Yuri	da Cunha	moreiraian@example.com	PF	2015-08-17 12:19:00-03	250.687.913-77	2004-10-08	Favela de Novaes, 18 Inconfidência 91458-864 Cardoso dos Dourados / RR	47030105
304	Diogo	da Cunha	nfogaca@example.net	PF	2019-07-04 08:12:00-03	916.073.425-61	1970-12-12	Lago de Aragão, 95 Vila São Dimas 49739-793 Cardoso de Cardoso / GO	75319572
333	Kaique	da Cunha	isaac30@example.com	PF	2019-12-03 10:34:00-03	975.423.018-88	1996-09-17	Quadra de Nunes, 726 Antonio Ribeiro De Abreu 1ª Seção 40430-944 Mendes do Oeste / ES	49961162
365	Nicole	da Cunha	meloana@example.com	PF	2020-03-11 10:52:00-03	420.187.596-20	1955-01-07	Largo de da Cruz, 327 Heliopolis 49762-696 Carvalho / AP	06435720
367	Vitor Hugo	da Cunha	carvalholuana@example.org	PF	2015-08-21 08:07:00-03	834.071.596-84	2003-09-13	Condomínio Cavalcanti, 18 Vila São Gabriel Jacui 74955699 da Conceição Verde / PE	20597-412
411	Vitor Hugo	da Cunha	luizafogaca@example.net	PF	2022-03-10 07:33:00-03	149.203.867-96	1980-09-21	Estação Cauã Dias, 563 Bairro Das Indústrias Ii 55294234 Ramos Alegre / RO	64285-091
569	Pedro Miguel	da Cunha	francisconunes@example.net	PF	2016-11-25 10:47:00-02	415.687.320-07	2000-12-21	Avenida da Rosa, 43 São Vicente 01035037 da Costa de Nogueira / MS	49502-391
736	Marcela	da Cunha	rteixeira@example.com	PF	2021-11-06 12:31:00-03	685.793.240-92	1990-04-24	Loteamento de Carvalho, 15 Jardim Felicidade 63256949 Pires / PI	81600217
801	Heloísa	da Cunha	peixotosophie@example.net	PF	2019-09-27 12:29:00-03	097.412.586-58	1947-02-24	Chácara de Porto Aparecida 7ª Seção 52508317 Farias das Pedras / AM	63541-173
854	Davi Luiz	da Cunha	carolinarocha@example.net	PF	2017-04-13 09:22:00-03	390.541.862-24	1984-12-29	Praça Marcelo Cardoso, 801 Vila Havaí 59749622 Gonçalves de Moraes / TO	09608-657
979	Elisa	da Cunha	bianca80@example.org	PF	2018-03-09 13:27:00-03	052.697.384-65	1964-05-21	Largo Cauê Costela, 57 Santa Terezinha 32928-221 Alves / ES	13852-001
127	Maria Cecília	da Rocha	goncalvesthiago@example.net	PF	2014-04-15 07:06:00-03	173.085.642-07	1956-03-29	Estrada de Moraes, 50 Vila Madre Gertrudes 4ª Seção 05905-261 Vieira Grande / RO	78505-017
131	Ana Vitória	da Rocha	sarah66@example.com	PF	2022-03-07 07:12:00-03	579.802.314-14	2003-10-07	Setor de Gomes Vila Da Ária 96571-576 Novaes / PI	05097-896
133	Sabrina	da Rocha	vitor-hugoda-cruz@example.com	PF	2022-12-22 07:05:00-03	418.652.709-11	1966-01-27	Lagoa Catarina Nascimento, 93 Marmiteiros 56518297 Souza da Praia / SE	82212-140
166	Vinicius	da Rocha	oliveirajoao-vitor@example.net	PF	2014-11-21 13:42:00-02	237.489.601-31	1962-07-08	Feira da Paz, 72 Jaraguá 20559043 Cunha de Barbosa / RR	83576382
223	Davi	da Rocha	nina15@example.org	PF	2022-12-17 13:43:00-03	861.450.927-85	1944-10-16	Rodovia de Novaes Nova Cachoeirinha 53720138 Fogaça / RJ	32671-446
299	Luiza	da Rocha	beatrizfarias@example.com	PF	2022-05-02 09:39:00-03	093.761.258-86	1964-09-01	Parque Pedro Henrique da Costa, 9 Vila São Rafael 06481-189 da Cruz / MT	99906-712
318	Renan	da Rocha	isabellabarbosa@example.org	PF	2017-03-06 09:50:00-03	895.416.370-00	1952-11-06	Praça de Almeida, 16 Vila São Gabriel 35718982 Rocha do Amparo / MT	65297-551
321	Enzo Gabriel	da Rocha	novaesbenicio@example.org	PF	2020-04-24 07:58:00-03	430.721.985-60	1984-09-10	Setor de das Neves, 91 Vila Sesc 49975573 Mendes das Flores / SP	26757418
342	Levi	da Rocha	cardosomariana@example.com	PF	2018-05-18 12:37:00-03	819.435.720-97	1955-07-26	Aeroporto Ramos, 66 Caiçaras 13805-115 Fernandes da Serra / AP	09963-625
402	Maria Eduarda	da Rocha	isaacfarias@example.org	PF	2021-07-15 09:44:00-03	579.820.436-74	1948-06-10	Trevo de Ribeiro, 45 Ouro Preto 65293730 Viana / DF	93842428
449	Valentina	da Rocha	bernardo59@example.net	PF	2017-11-10 10:04:00-02	543.916.782-09	1996-05-28	Fazenda Thiago Viana, 98 Marieta 3ª Seção 24022-676 Fogaça da Prata / PI	36323-611
479	Fernando	da Rocha	ana69@example.com	PF	2022-09-27 08:03:00-03	487.259.130-50	1944-01-03	Avenida Silveira Santa Cecilia 42240-995 Fernandes da Prata / CE	21886579
555	Maria Julia	da Rocha	joao-lucas62@example.com	PF	2012-08-24 09:57:00-03	703.184.265-44	1955-06-14	Alameda de Silva, 5 Tirol 97876009 Duarte / TO	58836369
557	Caio	da Rocha	da-motakaique@example.org	PF	2017-12-09 10:29:00-02	904.587.261-76	1970-02-21	Distrito Davi Lucas Peixoto, 50 Buritis 22871-810 Martins da Mata / AM	08567273
622	Sofia	da Rocha	pietra86@example.org	PF	2018-01-24 12:45:00-02	712.043.896-40	2000-07-15	Aeroporto Emilly Monteiro, 2 Saudade 02759639 Pereira Paulista / AL	55775-046
695	Stephany	da Rocha	heitor45@example.org	PF	2021-04-18 12:39:00-03	917.286.305-68	2005-05-20	Condomínio Cardoso, 86 Camponesa 2ª Seção 58777212 Cardoso / AM	58492135
700	Maitê	da Rocha	znogueira@example.com	PF	2017-12-06 14:13:00-02	376.218.950-12	2003-05-19	Trecho de Cunha, 69 Jaqueline 18973252 Silva Paulista / RO	13306021
727	Helena	da Rocha	da-cruzemanuelly@example.com	PF	2020-08-10 09:55:00-03	596.371.042-16	1969-06-07	Largo de Viana, 5 Maria Goretti 10929-994 Freitas / TO	95852-485
808	Esther	da Rocha	jlima@example.com	PF	2013-08-08 11:43:00-03	475.836.102-90	1946-06-09	Chácara da Mota Vila Mangueiras 58786767 Moreira da Mata / DF	15651001
835	Ana Vitória	da Rocha	wpinto@example.net	PF	2019-12-09 08:43:00-03	521.396.870-86	2004-10-03	Pátio de Teixeira São Salvador 81363-722 Costa / AM	48804710
840	Rebeca	da Rocha	yagoda-paz@example.org	PF	2013-02-05 09:08:00-02	356.271.984-19	1983-11-02	Praça Cunha, 6 Novo São Lucas 97233-609 Costa / RR	84203-810
850	Yago	da Rocha	rezendemaria-alice@example.net	PF	2013-07-22 11:42:00-03	123.765.894-28	1991-08-29	Favela Rodrigues, 62 Providencia 94259-772 Nogueira / CE	47657-827
937	Antônio	da Rocha	levi75@example.net	PF	2022-06-16 08:01:00-03	687.439.015-57	1952-07-15	Viela da Cunha, 862 Canadá 07585-026 Souza / RS	77420111
960	Yasmin	da Rocha	peixotolevi@example.net	PF	2014-07-05 10:14:00-03	435.106.278-90	1978-09-07	Distrito de Fernandes Conjunto Novo Dom Bosco 58539-369 Fogaça / MA	90446-968
980	Agatha	da Rocha	kevinsilveira@example.net	PF	2022-07-27 08:26:00-03	369.172.504-06	1981-11-29	Vila de Novaes, 31 São José 33867284 Porto da Praia / MG	06282416
59	Kevin	Fernandes	esthermoura@example.com	PF	2017-03-10 09:19:00-03	952.718.630-77	1978-10-26	Favela Maria Vitória da Mata, 459 Vila Nova Cachoeirinha 1ª Seção 87346483 Oliveira Alegre / SC	03392871
76	Gustavo Henrique	Fernandes	manuelacunha@example.org	PF	2015-11-06 13:36:00-02	792.861.034-50	1997-08-24	Colônia de Santos, 52 Lajedo 19356146 Azevedo Alegre / ES	06842-404
122	Cauê	Fernandes	joanacunha@example.net	PF	2016-10-02 08:23:00-03	412.509.378-41	1961-03-19	Praça de Moraes Jardim Vitoria 00036-106 da Rocha de Correia / GO	27901042
124	Daniela	Fernandes	enzo-gabrielmoura@example.com	PF	2021-03-27 11:20:00-03	206.487.931-50	1958-05-13	Alameda Dias Vila Trinta E Um De Março 11447-016 Ferreira / GO	28155450
147	Antônio	Fernandes	danielazevedo@example.com	PF	2014-08-14 11:32:00-03	046.935.278-74	1953-06-25	Largo Ana Sophia Teixeira, 29 Vila Paraíso 79124-838 Oliveira Verde / MT	57012244
189	Gustavo	Fernandes	gabrielapeixoto@example.org	PF	2021-09-16 08:47:00-03	082.674.591-11	2004-08-03	Alameda de Souza, 517 São Jorge 3ª Seção 05667856 Freitas da Prata / MA	94005-165
197	Heloísa	Fernandes	qbarros@example.org	PF	2020-09-26 09:01:00-03	963.421.870-96	1979-01-18	Rodovia da Mata, 417 Conjunto Providencia 29046-603 Sales / CE	77061439
206	Elisa	Fernandes	alveskaique@example.org	PF	2018-01-05 14:28:00-02	960.135.428-05	1995-12-24	Viaduto de Pires, 51 Belmonte 31151-666 Mendes / SE	05824-490
416	João Felipe	Fernandes	rezendeclarice@example.org	PF	2022-12-18 10:37:00-03	197.642.358-91	1969-05-18	Parque Júlia Gomes, 352 Minaslandia 02943662 Melo / CE	83554-241
419	Amanda	Fernandes	aragaonicolas@example.net	PF	2013-05-01 12:18:00-03	742.569.810-20	1997-01-06	Vereda Fogaça, 13 Vila Da Luz 75689427 Silva / AM	82890534
432	Gabrielly	Fernandes	isadoracastro@example.com	PF	2011-04-27 09:38:00-03	650.849.132-60	1979-08-14	Trecho de Jesus, 38 Floresta 36618215 Barbosa de Melo / AM	69614-039
434	Daniel	Fernandes	carvalhogustavo-henrique@example.com	PF	2021-08-02 12:38:00-03	928.563.047-29	1981-05-26	Largo Oliveira, 500 Barão Homem De Melo 3ª Seção 02940978 Gonçalves / SP	01623853
517	Pedro Lucas	Fernandes	sarahrocha@example.net	PF	2020-12-11 12:57:00-03	128.304.976-78	1999-11-16	Conjunto de Ribeiro, 83 Ventosa 23608423 Fernandes / ES	12242-958
559	Clara	Fernandes	augustonascimento@example.org	PF	2011-02-05 12:40:00-02	487.623.901-04	1965-01-07	Setor Davi Lucas Barbosa, 82 Santa Rita De Cássia 03726-574 Melo / CE	91130-743
577	Brenda	Fernandes	juliateixeira@example.com	PF	2016-03-15 13:07:00-03	039.461.875-01	1989-12-29	Estação Freitas, 981 Trevo 83536-825 Barros de Cunha / RS	67308-317
620	Juan	Fernandes	dsilveira@example.net	PF	2010-06-21 13:42:00-03	613.527.098-21	1988-02-16	Passarela Fogaça, 2 São Lucas 49846755 Pires / DF	19210293
789	Maitê	Fernandes	bsales@example.net	PF	2021-09-04 11:30:00-03	861.974.305-84	1996-07-20	Esplanada de Sales Apolonia 85934554 Aragão de Nascimento / AM	00215118
9	Ana Lívia	Rodrigues	ana-carolinacardoso@example.com	PF	2022-12-08 08:21:00-03	426.180.953-24	2003-10-04	Estrada de Costa, 56 Dona Clara 39213765 Cavalcanti / RN	29668-757
72	Daniel	Rodrigues	marcelacarvalho@example.net	PF	2019-04-07 12:08:00-03	864.103.529-05	1991-02-17	Estrada Gonçalves, 97 Vila Maria 10156-663 Rodrigues das Flores / PI	64726669
285	Clara	Rodrigues	paragao@example.net	PF	2017-08-06 10:36:00-03	153.946.078-93	2005-10-20	Via Ian Pereira Vila Petropolis 33784870 Souza de Nogueira / PI	63949-164
317	Benício	Rodrigues	ana-julia87@example.com	PF	2020-08-17 11:35:00-03	683.279.450-92	1999-09-30	Condomínio Araújo, 62 São Jorge 1ª Seção 67770-816 Teixeira / MS	56004-247
332	Alexia	Rodrigues	alexandrenovaes@example.org	PF	2020-09-10 08:54:00-03	958.712.043-41	1964-01-22	Jardim Campos Vila Vista Alegre 72946-280 Freitas / RR	60379109
454	Lorena	Rodrigues	maria-alicecostela@example.net	PF	2022-09-12 10:27:00-03	547.826.319-28	1949-05-19	Vereda Melo, 6 Lindéia 47435530 Cunha da Prata / PR	17495865
471	Ana Sophia	Rodrigues	lara41@example.com	PF	2022-10-20 13:02:00-03	826.405.931-70	1979-02-27	Vereda da Mata, 299 Outro 28267-434 Porto / MA	10973112
494	Raquel	Rodrigues	ppires@example.net	PF	2019-09-05 08:39:00-03	609.245.183-24	1949-01-23	Via Lima, 4 Nova Pampulha 87659083 da Mata dos Dourados / PR	34903-374
527	Yasmin	Rodrigues	tbarros@example.net	PF	2015-08-10 09:14:00-03	296.407.315-52	1966-02-15	Residencial Pedro Henrique da Luz, 71 Vila Nossa Senhora Do Rosário Ápia 64249265 Monteiro / SE	48563-639
681	Rafael	Rodrigues	pedro11@example.com	PF	2018-07-23 10:19:00-03	391.450.267-34	1943-11-30	Passarela de Martins, 8 Universo 59745073 Farias do Amparo / SP	43914-408
740	Ana Júlia	Rodrigues	peixotomirella@example.com	PF	2019-11-12 09:14:00-03	671.825.943-82	1957-09-26	Viela da Luz, 80 Monte São José 87192675 da Rosa / PB	42264-376
955	Daniel	Rodrigues	cardosovalentina@example.net	PF	2020-03-28 11:02:00-03	742.915.036-52	1966-06-23	Conjunto de Costa, 90 Nova Floresta 93852559 da Rocha / PB	99955777
963	Pietra	Rodrigues	enzo-gabrielgoncalves@example.com	PF	2022-06-13 12:07:00-03	916.805.342-89	1966-02-20	Praça João Gabriel Cunha, 88 Conjunto Minas Caixa 23113-653 Barros da Serra / PB	22647-853
107	André	das Neves	qcunha@example.net	PF	2020-04-19 09:09:00-03	983.617.452-46	1984-01-03	Trevo de Novaes Vila Do Pombal 54247-469 Rezende / SE	27706384
134	Maria Vitória	das Neves	natalia68@example.com	PF	2013-06-11 08:41:00-03	187.094.365-10	1987-07-27	Sítio de Farias, 2 Belvedere 68437-729 Silveira da Prata / PR	10557-284
146	Kaique	das Neves	henriquefreitas@example.net	PF	2010-06-27 07:33:00-03	479.126.835-00	1995-10-26	Praça Nogueira, 63 Vila Da Ária 72667-639 Fernandes / RJ	79069920
186	Maria Fernanda	das Neves	fariasdavi@example.net	PF	2022-02-21 12:05:00-03	673.952.081-02	1996-04-27	Vila Barros Vila Suzana Primeira Seção 23620-328 da Rocha / AP	80368790
280	Marcelo	das Neves	ymartins@example.com	PF	2017-04-20 11:07:00-03	520.798.164-11	1994-12-06	Esplanada Rodrigues, 5 Custodinha 25382-390 Nascimento / AL	97755-778
346	Ian	das Neves	giovanna21@example.net	PF	2019-03-02 08:07:00-03	895.364.710-00	1948-05-11	Via Marcos Vinicius da Paz, 979 Santa Amelia 83619-937 das Neves / AM	54965-372
368	Agatha	das Neves	marina82@example.net	PF	2016-03-03 10:31:00-03	285.094.317-79	1995-08-19	Quadra de Martins, 43 Solimoes 18053489 Freitas / AL	22657-163
373	Matheus	das Neves	ribeirovitoria@example.com	PF	2013-07-19 12:18:00-03	501.384.729-04	1972-07-18	Vila de Cardoso, 69 Cidade Jardim 68975-875 Vieira de Azevedo / MS	71829-681
384	Levi	das Neves	otavio49@example.net	PF	2022-03-10 10:20:00-03	860.597.234-38	1968-02-19	Ladeira Nina da Cunha, 44 Mangabeiras 98491-703 da Costa de da Cruz / MS	20679-948
398	Anthony	das Neves	lorenzomoraes@example.com	PF	2022-02-16 11:30:00-03	069.243.187-03	1988-11-06	Alameda Eduardo Silva, 218 Diamante 19328663 Melo / MA	97811-552
582	Enzo	das Neves	lucascorreia@example.net	PF	2013-02-21 08:58:00-03	390.245.168-89	1969-07-16	Residencial de Ribeiro, 10 Vila Da Luz 07602-791 Costela da Serra / BA	55858-197
656	Isabel	das Neves	wpeixoto@example.net	PF	2019-02-20 09:24:00-03	915.860.432-42	1963-01-13	Condomínio Luna Castro, 57 Vila Formosa 96423-311 Rezende de Minas / SP	59101561
694	Sophie	das Neves	agathada-rocha@example.org	PF	2022-04-07 11:35:00-03	125.043.687-71	1999-04-29	Loteamento Campos, 929 Vila Nova Gameleira 3ª Seção 66001725 Correia / PE	26812-022
781	Luiz Henrique	das Neves	alvesbruna@example.net	PF	2014-09-23 08:27:00-03	459.871.326-73	1975-11-28	Área da Luz, 68 Vila Pilar 86948582 Mendes / RS	50116-459
862	Kaique	das Neves	leandroporto@example.net	PF	2017-11-07 09:35:00-02	297.035.814-04	1949-02-11	Via Jesus Vila Piratininga 32138-209 Fernandes / GO	26806-550
914	Enzo	das Neves	helena00@example.org	PF	2017-11-11 09:08:00-02	139.820.675-02	2006-04-28	Praça da Costa, 42 Olhos D'água 81626230 Novaes / AM	60188-798
944	Lorenzo	das Neves	fnovaes@example.org	PF	2020-05-09 08:42:00-03	076.829.154-20	1985-04-27	Vila Ana Laura Carvalho, 4 Belmonte 19320100 Melo / BA	55765-821
139	Júlia	Cavalcanti	ofogaca@example.org	PF	2016-09-23 11:05:00-03	798.140.625-02	1997-03-21	Estação de Cardoso, 56 Dom Silverio 13344012 Ferreira de Cunha / AP	39833273
158	Leonardo	Cavalcanti	luiz-otavioporto@example.net	PF	2018-10-22 12:47:00-03	429.061.875-85	2006-11-25	Ladeira da Costa, 43 Lorena 14774659 Carvalho / SP	26418948
172	Camila	Cavalcanti	yagodias@example.com	PF	2020-06-20 12:24:00-03	054.629.781-11	1958-08-30	Recanto de Gonçalves, 16 Esperança 61141526 Correia / DF	23385183
231	Stella	Cavalcanti	novaesmirella@example.org	PF	2012-06-25 08:43:00-03	207.415.369-43	1982-12-15	Chácara Guilherme Gomes, 2 Paulo Vi 47958-342 Alves de Pinto / AM	30778558
306	Ana Lívia	Cavalcanti	teixeiraigor@example.org	PF	2012-05-23 09:11:00-03	345.170.986-48	1973-05-13	Jardim de Cardoso, 394 Cenaculo 60649165 Gomes / CE	80854977
400	Maria Sophia	Cavalcanti	nunesenrico@example.org	PF	2018-04-21 12:16:00-03	843.692.517-37	1955-04-02	Ladeira de Teixeira, 19 Savassi 82558-456 Almeida / RS	88267-114
478	Theo	Cavalcanti	novaesian@example.net	PF	2022-06-03 07:52:00-03	026.817.943-31	1944-10-03	Passarela Pires Vila Batik 77077-693 Novaes das Flores / GO	49158046
514	Carolina	Cavalcanti	souzanoah@example.net	PF	2021-01-20 08:35:00-03	643.058.172-17	1943-06-27	Rodovia Rodrigues, 47 Vila Da Luz 94063221 Barbosa / MS	41606515
551	Kamilly	Cavalcanti	xcosta@example.com	PF	2018-03-13 08:48:00-03	801.765.492-85	1945-08-25	Fazenda de da Luz, 32 Vila De Sá 46662088 Barros / PB	11817-226
721	Ryan	Cavalcanti	stella50@example.com	PF	2022-07-17 11:30:00-03	396.147.052-99	1990-12-24	Jardim Vinicius da Rosa, 42 Venda Nova 40605-848 Barros / TO	87324-761
759	Caio	Cavalcanti	mendesbrenda@example.com	PF	2022-01-02 11:41:00-03	713.480.965-00	1991-02-07	Lago Vitor da Rosa, 61 Santa Terezinha 62952423 da Paz / MA	72563332
909	Rafael	Cavalcanti	sabrinapeixoto@example.org	PF	2019-01-04 11:26:00-02	261.438.509-33	1972-05-31	Sítio de Campos, 34 Pongelupe 95037288 Ramos da Serra / RR	12331-285
929	Lucas	Cavalcanti	enzoporto@example.com	PF	2019-02-27 10:07:00-03	679.183.052-21	1966-10-19	Viela Stephany Rezende, 44 Conjunto São Francisco De Assis 64551909 Moraes da Praia / SE	37186-965
939	Eduardo	Cavalcanti	nicolas66@example.org	PF	2022-12-13 10:16:00-03	948.376.150-66	1959-03-25	Setor Oliveira, 46 Coqueiros 58476112 Gomes de Peixoto / RR	42099413
942	Heitor	Cavalcanti	rgoncalves@example.com	PF	2015-10-24 13:54:00-02	670.382.591-21	1949-11-15	Vila Lima, 47 Vitoria Da Conquista 58876-621 Silveira / RS	46154-666
977	Bryan	Cavalcanti	maria-cecilia77@example.com	PF	2018-12-15 11:03:00-02	264.091.537-16	1975-09-12	Recanto Ana Sophia Campos, 34 Vera Cruz 07806-780 Freitas / MS	27938-464
999	Vicente	Cavalcanti	das-neveslucca@example.com	PF	2017-08-25 12:13:00-03	793.065.481-84	1972-03-02	Quadra Novaes, 72 Caiçara - Adelaide 53166402 Gonçalves / SE	90205-329
61	Matheus	Gonçalves	joao-lucasdas-neves@example.org	PF	2015-11-19 13:34:00-02	347.621.509-16	1951-08-03	Trecho Aragão, 88 Vila Inestan 41949302 Almeida / PA	27808532
141	Ana Lívia	Gonçalves	nathan09@example.com	PF	2012-05-10 10:49:00-03	108.379.564-39	1963-06-22	Fazenda Azevedo Vila Sumaré 59970-897 Gonçalves do Sul / ES	55263-600
313	Helena	Gonçalves	anunes@example.org	PF	2022-08-22 10:17:00-03	937.684.510-20	1981-12-30	Parque Gustavo Henrique Mendes, 4 Boa União 2ª Seção 56221170 Cardoso do Amparo / AP	81315-884
437	Erick	Gonçalves	catarinacardoso@example.net	PF	2022-10-27 08:52:00-03	508.274.931-50	1985-11-16	Vereda de Lima, 55 Vila Suzana Primeira Seção 15460446 Fernandes / PA	05964-403
469	Maria Sophia	Gonçalves	heitorsouza@example.net	PF	2018-09-27 08:50:00-03	138.096.254-42	1974-05-18	Praça Levi Peixoto, 291 São Pedro 94053525 Jesus / PI	79618894
540	Stella	Gonçalves	vitor-gabrielcunha@example.net	PF	2018-12-07 14:22:00-02	207.936.851-68	1993-08-16	Esplanada Rodrigues, 96 Tres Marias 13147506 Fogaça do Amparo / PA	25815457
563	Lívia	Gonçalves	rcarvalho@example.org	PF	2021-07-10 08:51:00-03	310.965.247-16	1963-10-07	Área Pires, 49 Barão Homem De Melo 3ª Seção 77234-918 Lopes de da Rosa / AL	58701124
589	Anthony	Gonçalves	vcunha@example.org	PF	2018-09-13 10:09:00-03	804.267.913-04	1978-04-11	Residencial de Cardoso, 60 Coqueiros 39605768 Moraes Paulista / MG	15001-358
631	Vitor Hugo	Gonçalves	nicolesouza@example.com	PF	2010-12-10 13:06:00-02	465.270.389-92	1955-07-24	Viaduto de Novaes, 12 Novo Glória 04915531 da Rosa / SC	57442026
659	Vicente	Gonçalves	isiscunha@example.org	PF	2011-09-22 11:33:00-03	325.904.168-06	1955-12-14	Favela Ana Laura Oliveira, 5 Santo Antônio 04274-687 Nogueira / SE	65755-758
827	Thales	Gonçalves	oaraujo@example.org	PF	2022-10-24 11:44:00-03	124.307.956-80	1958-09-22	Setor das Neves, 72 Santa Branca 71259-487 Novaes / ES	50201054
897	Gustavo	Gonçalves	clariceduarte@example.net	PF	2016-12-07 09:46:00-02	346.529.801-24	1952-04-14	Praça de Mendes, 4 Goiania 40515-711 Moreira de Ribeiro / AM	92732966
87	Diogo	Nascimento	vcostela@example.net	PF	2020-07-08 11:09:00-03	397.420.186-69	1993-04-13	Estação Fogaça, 35 Parque São José 81160-035 Farias Alegre / PA	51797436
126	Vinicius	Nascimento	tgomes@example.net	PF	2017-07-25 09:04:00-03	391.687.402-04	1965-03-30	Colônia Vieira, 305 Conjunto Jatoba 76544-562 Cunha de Rezende / BA	85510-255
170	Letícia	Nascimento	pedro65@example.com	PF	2022-12-15 13:01:00-03	534.968.102-89	1961-04-08	Vale Ribeiro, 46 Bandeirantes 28426494 Monteiro / AM	58593-834
235	João Vitor	Nascimento	ramosdiego@example.com	PF	2018-06-01 10:05:00-03	805.319.427-32	1951-03-09	Quadra Nicole Fogaça, 609 Nossa Senhora De Fátima 35586-737 Nunes das Flores / DF	63334006
267	Milena	Nascimento	fogacagustavo@example.net	PF	2022-09-12 08:33:00-03	638.940.725-47	1983-10-20	Via de Cardoso, 769 Palmares 58160221 Nascimento / PR	85923-196
283	Vitória	Nascimento	hda-costa@example.org	PF	2018-10-13 09:41:00-03	965.142.738-82	1981-01-05	Estação de Barros, 44 Laranjeiras 75970597 Fernandes da Mata / PB	88118-294
370	Camila	Nascimento	araujocaue@example.org	PF	2022-07-08 10:14:00-03	027.465.398-29	1954-12-25	Largo Francisco Campos, 121 Barro Preto 43462444 da Rosa do Norte / RJ	10958-170
535	Anthony	Nascimento	mda-rosa@example.org	PF	2014-09-06 10:52:00-03	369.478.210-96	1982-06-05	Sítio de Costa, 8 Tiradentes 20333-454 Barbosa / RR	72930-493
558	Valentina	Nascimento	lorenzoda-mota@example.com	PF	2021-08-23 09:15:00-03	530.978.214-14	1988-11-08	Trecho da Mota, 23 Grotinha 62716625 Gonçalves Alegre / RR	87493-182
596	Marina	Nascimento	catarina49@example.net	PF	2022-08-06 10:27:00-03	735.160.948-93	1960-01-28	Praça Barros, 4 Unidas 18759785 Cardoso de Goiás / PR	89659-956
752	Isaac	Nascimento	pintoandre@example.com	PF	2017-10-28 13:26:00-02	850.326.417-07	1962-01-03	Largo Novaes, 62 Alípio De Melo 10978-312 Castro / RO	03366293
35	Caio	da Conceição	xaraujo@example.com	PF	2015-09-11 09:06:00-03	691.534.802-60	1965-06-24	Trecho Gomes, 615 Concórdia 78034-295 Nascimento do Norte / MA	06463-522
93	Yasmin	da Conceição	isabellypeixoto@example.com	PF	2021-10-27 08:09:00-03	617.839.540-00	1980-06-03	Núcleo de Barbosa, 483 Santo André 73918-122 Araújo Alegre / AM	98928640
118	Letícia	da Conceição	ada-mata@example.net	PF	2022-04-14 09:33:00-03	564.210.389-05	2004-07-01	Viaduto de Barbosa, 97 Nova America 88034834 Silva / MS	62039-437
162	Ana Carolina	da Conceição	bruno11@example.com	PF	2012-11-15 09:37:00-02	426.875.319-28	1981-08-06	Vereda da Luz, 34 Cachoeirinha 24441510 Campos / RJ	80497-612
311	Clara	da Conceição	pda-paz@example.org	PF	2021-10-13 09:56:00-03	927.458.036-38	1952-08-25	Estação Moura, 903 Olhos D'água 09013-005 Barros / RR	08342-543
325	Enrico	da Conceição	maria-julia86@example.org	PF	2014-04-14 12:34:00-03	356.179.240-52	1945-03-31	Alameda de Ferreira, 35 Vila Nova Gameleira 1ª Seção 10307956 Campos de Carvalho / AM	13633730
339	Luana	da Conceição	ssantos@example.com	PF	2019-10-13 12:10:00-03	149.072.586-58	1944-04-13	Vila Cavalcanti, 8 Milionario 04460296 Cardoso Verde / GO	72374771
453	Joaquim	da Conceição	kaique18@example.org	PF	2022-01-14 10:16:00-03	529.761.483-09	1988-11-28	Viaduto Bruno Dias, 68 Mariquinhas 02965-100 Rezende / GO	82998-298
541	Mirella	da Conceição	samuelmoura@example.net	PF	2019-04-25 10:04:00-03	479.068.315-00	1992-05-29	Lagoa Lucca da Luz, 81 Laranjeiras 49327056 Farias de Minas / TO	73849-470
594	Alexandre	da Conceição	oliveirathomas@example.com	PF	2019-11-26 11:01:00-03	692.384.157-73	1998-07-07	Sítio Teixeira, 17 Vila Engenho Nogueira 44233382 Duarte de Barros / MA	91422-196
629	Samuel	da Conceição	ana-laura75@example.com	PF	2018-06-15 10:15:00-03	597.183.420-79	1943-02-09	Conjunto Sales, 85 Santo Agostinho 25555778 Alves / AC	61134-341
643	Júlia	da Conceição	larissa33@example.com	PF	2022-03-26 07:46:00-03	573.916.402-80	1961-04-26	Ladeira de Ferreira Nova Pampulha 71101341 Azevedo do Oeste / AP	88015872
671	Carlos Eduardo	da Conceição	alexandre92@example.net	PF	2021-08-09 13:15:00-03	419.076.238-50	1969-12-19	Viela Araújo, 98 Xodo-Marize 64207859 Teixeira / GO	75614912
730	Bárbara	da Conceição	enzo-gabrielcardoso@example.com	PF	2019-05-02 08:22:00-03	019.385.267-59	1966-03-06	Chácara de Martins, 85 Mantiqueira 26805-573 Azevedo do Norte / SC	08264521
772	Daniel	da Conceição	raquel97@example.com	PF	2017-06-08 08:13:00-03	240.581.396-33	1971-07-20	Pátio de Souza, 8 Vila Puc 47056676 Duarte / MS	55045-265
837	Theo	da Conceição	ninacastro@example.org	PF	2014-03-18 07:30:00-03	058.734.129-79	1944-06-03	Loteamento Pires, 4 Jardim Felicidade 69804-761 Nunes de da Cruz / RR	88159-361
845	Pedro Lucas	da Conceição	silveirabruno@example.com	PF	2020-12-16 12:14:00-03	347.028.591-88	1943-12-12	Travessa Eloah Moura, 97 Alta Tensão 2ª Seção 77898054 Moreira / PE	36211-005
853	Milena	da Conceição	ribeirodanilo@example.net	PF	2020-11-21 08:32:00-03	870.534.912-50	1999-04-22	Feira de Vieira, 1 Planalto 31794137 Rodrigues / RO	15386938
\.


--
-- Data for Name: colaborador_agencia; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.colaborador_agencia (cod_colaborador, cod_agencia) FROM stdin;
7	1
13	1
14	1
23	1
24	1
30	1
31	1
34	1
42	1
58	1
62	1
67	1
72	1
77	1
79	1
83	1
12	2
15	2
22	2
28	2
46	2
56	2
57	2
63	2
65	2
69	2
70	2
81	2
96	2
4	3
27	3
37	3
41	3
45	3
47	3
49	3
74	3
78	3
88	3
98	3
3	4
32	4
61	4
73	4
5	5
9	5
10	5
21	5
36	5
43	5
50	5
52	5
55	5
59	5
60	5
76	5
92	5
2	6
16	6
17	6
29	6
44	6
53	6
54	6
66	6
87	6
93	6
97	6
20	7
26	7
48	7
64	7
84	7
85	7
86	7
33	8
35	8
40	8
51	8
68	8
80	8
94	8
1	9
11	9
25	9
38	9
39	9
75	9
90	9
91	9
6	10
8	10
18	10
19	10
71	10
82	10
89	10
95	10
99	10
100	10
\.


--
-- Data for Name: colaboradores; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.colaboradores (cod_colaborador, primeiro_nome, ultimo_nome, email, cpf, data_nascimento, endereco, cep) FROM stdin;
14	Paulo	Dias	melissalopes@example.net	847.210.695-02	1974-04-24	Lagoa de Rodrigues, 62 Zilah Sposito 37328-273 Castro Paulista / PE	27275674
19	Luiz Fernando	Dias	pcunha@example.net	820.415.963-33	1994-07-10	Ladeira de Moreira, 82 Jardim Atlântico 27617-762 Sales Verde / PB	50013962
32	Vitor Hugo	Dias	aragaonicolas@example.net	936.172.548-37	1986-12-18	Área Sophie Moreira Maria Virgínia 71941-111 Souza do Campo / RJ	08955-215
88	João Gabriel	Lima	luiz-otaviorezende@example.org	967.082.451-67	1994-10-19	Trevo Murilo Ramos, 92 São Benedito 73615-597 Moura da Mata / RR	24224-305
39	Maria Eduarda	Melo	joao-pedro72@example.com	857.496.210-49	1996-03-01	Parque de Moura, 65 Granja Werneck 67769059 da Rosa de da Cunha / RR	96896390
41	Olivia	Costa	sabrina59@example.com	190.768.435-20	1966-01-30	Favela Rocha, 94 Laranjeiras 92500120 Viana / GO	90019993
45	Laura	Cunha	lviana@example.org	217.583.096-95	1977-02-07	Lagoa Benício Moura, 5 Vila Paraíso 50235854 da Conceição / RJ	19415-341
100	Lavínia	Cunha	noahmoraes@example.com	062.937.158-02	1975-03-24	Colônia Lima, 83 Flamengo 36172-665 da Mota / SE	74890752
16	Benjamin	Gomes	vjesus@example.org	718.950.436-20	1963-12-21	Conjunto Gomes, 888 Céu Azul 76308-427 Lopes do Norte / GO	29604844
46	Alana	Gomes	samuel76@example.com	768.134.950-20	1977-07-16	Fazenda Emilly Costa Campo Alegre 55916-800 da Cunha de Rocha / SE	88161-168
76	Ana Júlia	Gomes	davi-luccacardoso@example.org	461.837.095-57	1985-09-13	Viaduto Rezende, 33 Beira Linha 94699659 Fogaça do Oeste / RN	73793-541
64	Ana Júlia	Jesus	thales22@example.com	265.931.407-16	1975-06-15	Travessa Araújo Flamengo 90045246 Dias / AM	85994735
86	Eduarda	Jesus	zmelo@example.com	260.431.897-04	1983-10-28	Vale de Ribeiro, 6 Vera Cruz 77263-255 Lopes Verde / MS	53119-377
91	Emilly	Jesus	freitascaroline@example.com	053.126.487-44	1985-03-03	Pátio Aragão, 171 Califórnia 42079209 da Rosa Grande / ES	95399-857
4	Laís	Lopes	diaslucas@example.org	269.580.741-49	1976-10-14	Sítio da Paz, 94 Conjunto Jatoba 59169-769 Nogueira / PA	07868885
42	Enrico	Lopes	oviana@example.com	196.574.038-39	1991-03-23	Chácara de Pereira, 148 Etelvina Carneiro 74386-933 das Neves / TO	98758451
53	Helena	Lopes	antoniomonteiro@example.org	928.706.341-96	1990-10-08	Área Enrico Caldeira, 9 Santa Cecilia 37808172 Rocha da Prata / PB	48143-984
57	Lucca	Lopes	das-nevesmaria-fernanda@example.org	697.351.804-48	1966-02-21	Área de da Cunha, 52 Pantanal 27949-577 Barros / MT	04775-545
67	Luiz Henrique	Lopes	davi-luiz75@example.net	538.697.210-30	1990-05-28	Avenida Barros, 820 Vila Puc 71371716 da Paz / AM	95994401
56	Julia	Pinto	jalves@example.org	734.206.859-47	1985-11-27	Trecho Teixeira, 90 Vila Oeste 93070-834 Moraes Paulista / DF	71279-339
31	Joaquim	Pires	laispeixoto@example.org	104.263.875-62	1978-06-12	Loteamento de Peixoto, 42 Aparecida 7ª Seção 72660123 Nunes / SE	51569-010
43	Marcelo	Ramos	heitor34@example.org	279.438.056-74	1968-11-13	Praia Daniela Nunes, 10 Vila Trinta E Um De Março 92370116 Silva Grande / AP	74002-728
50	João Pedro	Ramos	barbara58@example.com	814.096.532-42	1970-04-09	Fazenda de Pinto, 27 Pirineus 57057184 da Paz / MS	36777-821
26	Luiz Henrique	Rocha	rodriguesolivia@example.net	934.210.785-05	1974-06-03	Alameda de Fogaça, 60 Araguaia 33543-326 Moraes de Santos / CE	26210551
36	Marcela	Rocha	henriqueda-luz@example.com	503.247.891-14	1963-07-16	Ladeira de Porto, 9 Distrito Industrial Do Jatoba 71922-717 Lima de Souza / SE	42103-068
85	Pedro Lucas	Rocha	pauloribeiro@example.org	305.687.219-77	1991-12-07	Fazenda Pedro Miguel Vieira, 714 São Salvador 88997-693 Viana da Mata / RR	12941395
22	Davi Luiz	Sales	leticia48@example.org	192.058.346-70	1979-01-07	Loteamento de Fogaça, 485 Vila Fumec 29814669 Gonçalves do Oeste / DF	60281-719
54	João Gabriel	Sales	emanuella17@example.net	784.290.531-23	1985-04-23	Ladeira de Viana, 1 Conjunto Capitão Eduardo 97321062 Dias / BA	16683185
82	Mirella	Sales	pedro-miguelda-rocha@example.org	097.435.216-07	1967-11-13	Loteamento Monteiro Conjunto Bonsucesso 77655-152 Nogueira de Minas / MA	62738233
65	Sabrina	Silva	luiz-gustavo49@example.org	652.307.918-02	1990-02-15	Lago Costela, 31 Conjunto Santa Maria 75957-012 Azevedo / PE	75913-235
68	João Miguel	Souza	icarvalho@example.com	627.134.095-43	1962-10-14	Passarela Natália da Rosa, 24 São Pedro 65884-698 Rocha da Praia / PI	98839-500
84	Pietro	Souza	diogo68@example.org	136.078.249-40	1985-03-31	Sítio Pereira, 2 Baleia 52815-925 da Cruz / PB	92408-486
72	Noah	Barros	martinsmarcos-vinicius@example.net	126.749.853-64	1991-04-25	Colônia de da Luz, 258 Vila Santo Antônio Barroquinha 53260-352 Farias / MG	57387761
62	Arthur	Campos	oviana@example.org	381.657.092-59	1973-12-28	Conjunto Alves, 8 Santa Amelia 36469-502 da Mota do Amparo / BA	16596-292
3	Kamilly	Castro	correialivia@example.net	297.634.180-03	1975-09-15	Praia Marina Monteiro, 29 Vila São Francisco 58603414 Vieira da Serra / SC	96322-716
17	Larissa	Castro	asilva@example.com	137.405.962-52	1980-05-19	Setor de Oliveira, 8 Teixeira Dias 68695-507 Costela / SC	71170-459
69	Helena	Castro	caldeiragiovanna@example.org	714.309.582-60	1986-10-05	Trevo de Cavalcanti, 9 Renascença 86582782 Lima do Campo / RJ	26544-466
98	Alana	Castro	rodrigo08@example.org	825.140.697-85	1964-03-06	Trecho de da Paz, 27 Conjunto Taquaril 72170-264 Pinto / MS	36787-854
18	Maria Vitória	Duarte	almeidacaroline@example.net	047.836.592-65	1963-12-24	Setor Thiago da Rocha, 9 Independência 55260-908 Santos da Prata / PE	29043497
81	Benício	Farias	maria91@example.com	059.817.632-21	1976-05-08	Avenida Melo, 40 Estrela Do Oriente 56389266 da Luz do Oeste / AM	52447-374
38	Luigi	Moraes	camposgiovanna@example.org	761.038.529-86	1981-02-15	Condomínio Mirella da Conceição, 19 Petropolis 14916934 Lopes / PE	83417203
63	Ana Clara	Moraes	vitoriagomes@example.org	291.847.506-85	1981-05-08	Favela de Oliveira, 514 Vila Nova Gameleira 3ª Seção 04170-829 Teixeira do Campo / CE	74407700
10	Vitor Hugo	Novaes	isisda-paz@example.org	187.053.296-12	1969-06-19	Pátio Silveira, 48 Concórdia 02998-381 Moraes / SC	75796-270
13	Maria Fernanda	Novaes	milena41@example.net	942.718.605-49	1964-04-13	Distrito Enrico Souza, 9 Vila Novo São Lucas 51945-621 Gonçalves de Cunha / MS	96640825
92	Eduarda	Novaes	da-rochagustavo@example.com	961.452.703-06	1982-12-19	Lagoa de Costa, 96 Conjunto Minas Caixa 87681-170 da Rosa / PE	54656-513
25	Lara	Santos	pteixeira@example.com	768.541.203-90	1970-05-13	Largo Nogueira, 59 Conjunto Bonsucesso 43484658 Porto Paulista / AM	68997-717
37	João Lucas	Santos	araujoana-vitoria@example.net	813.769.540-01	1981-07-04	Condomínio Araújo Vera Cruz 78170-346 da Paz da Serra / SE	75795-729
24	Vitória	Vieira	pedro-henrique56@example.org	572.340.961-16	1989-06-13	Campo Bruno Melo, 3 São Jorge 1ª Seção 04133-564 Araújo de Pires / PE	92847600
44	Bruno	da Paz	wmoura@example.org	534.062.971-61	1997-10-30	Alameda de Aragão, 5 Monte Azul 28055-168 Jesus / SC	64363-566
30	Arthur	Almeida	joaquim97@example.org	057.264.198-20	1980-01-16	Travessa Vitor Gabriel Silveira, 20 Granja Werneck 96535821 das Neves da Mata / AC	69196806
90	Cauê	Almeida	wcampos@example.org	176.084.239-78	1993-12-03	Vale de Costa, 62 Acaiaca 71168-130 Gonçalves da Praia / MG	61301741
20	Alice	Aragão	pedro-henrique82@example.com	495.278.610-20	1964-09-19	Núcleo Emanuella Jesus, 621 Vila Jardim Montanhes 66803663 Pires do Campo / RR	23821831
74	Laís	Aragão	stephanypeixoto@example.net	027.619.358-02	1991-11-10	Distrito Pinto Liberdade 55152-938 Nascimento / PA	28501-411
29	Juan	Barbosa	cavalcantithomas@example.org	403.527.198-50	1967-10-09	Favela de Cunha, 84 Jardim América 28360-153 da Costa do Campo / RJ	42832-180
83	Julia	Barbosa	da-luzcarolina@example.com	149.853.026-51	1993-11-04	Esplanada de da Conceição, 52 Satelite 42238-305 Cavalcanti / DF	41497222
71	Gabriela	Cardoso	arthur02@example.net	385.049.162-51	1967-07-13	Colônia Thomas Viana, 918 Marajó 70771-248 Duarte / RJ	08566439
89	Camila	Cardoso	silveiradaniela@example.org	461.203.589-51	1965-10-21	Rodovia de Nogueira, 37 União 95858-378 das Neves / AM	65408-812
97	Nicolas	Cardoso	costajoao-miguel@example.net	897.605.341-93	1969-04-22	Vereda Freitas, 46 Minas Brasil 80891-631 da Mata / CE	21980-775
21	Rafaela	Correia	vitor-hugosales@example.org	390.752.168-40	1973-03-24	Chácara de Barbosa, 29 Engenho Nogueira 03345703 Cunha do Oeste / SP	03927-555
33	Elisa	Correia	leandro34@example.net	965.832.147-09	1993-02-23	Morro de Souza, 84 Distrito Industrial Do Jatoba 67203-288 da Mota / AP	50769-513
47	Maitê	Correia	carlos-eduardo92@example.com	031.248.796-78	1978-10-25	Lagoa de da Mata, 20 Silveira 49023-064 Rocha do Sul / PE	94709-044
6	Clarice	Costela	lrezende@example.org	264.875.013-44	1977-02-06	Parque de Silveira, 66 Vila Da Luz 09881-664 Jesus / MS	12204328
99	Fernando	Costela	osilveira@example.net	492.758.103-97	1963-11-24	Praça de Duarte, 5 Santa Rosa 84418424 Cardoso de Carvalho / RO	15164977
23	Elisa	Fogaça	qjesus@example.net	579.816.234-64	1988-08-16	Campo João Barros, 99 Providencia 47135-118 Melo de Lima / ES	12605284
96	Gustavo	Martins	emilly44@example.com	790.245.683-74	1995-09-02	Largo de das Neves, 1 Nova Esperança 60391950 Monteiro de da Rocha / CE	97215561
11	Camila	Moreira	enzo-gabrielduarte@example.com	263.589.174-55	1970-03-30	Lago Evelyn Teixeira, 7 Pantanal 68026-096 Carvalho / SP	92857-785
7	Letícia	Peixoto	igornovaes@example.net	396.071.582-02	1994-09-10	Ladeira da Costa, 86 Conjunto Bonsucesso 59864-562 Porto de Barros / ES	92465328
9	Diego	Peixoto	valentinada-rocha@example.net	367.528.901-02	1969-04-01	Sítio Maria Eduarda Pires, 8 Antonio Ribeiro De Abreu 1ª Seção 50991-334 Nogueira da Prata / BA	77896-815
40	Bryan	Peixoto	kgomes@example.com	047.931.256-70	1975-08-22	Rodovia Leonardo da Cunha, 135 Ventosa 29667758 Moreira do Norte / AP	79262-228
52	Theo	Peixoto	ncosta@example.net	578.310.962-21	1991-10-10	Fazenda Peixoto, 68 Marieta 3ª Seção 99059-719 Porto / SE	47431-701
2	Clarice	Ribeiro	noahcastro@example.org	091.367.842-22	1993-11-10	Recanto Nogueira, 3 Xangri-Lá 38163250 Vieira de Mendes / RN	05165-716
5	Emanuella	Ribeiro	paulo12@example.org	714.695.380-75	1974-06-01	Parque Lima, 3 Europa 70881-379 Cunha / RS	22588-498
1	Lorena	da Mata	luiz-felipefarias@example.org	843.201.579-23	1996-01-31	Vereda da Rocha, 240 Vila Aeroporto 10668-659 Caldeira / MT	69097997
79	Pietro	da Mata	luiza18@example.net	241.395.068-06	1973-04-24	Passarela de Peixoto Distrito Industrial Do Jatoba 34288-743 da Mata Paulista / AM	04555518
48	Pietra	da Mota	ianviana@example.net	452.389.017-04	1978-11-16	Trevo Bruno Azevedo Engenho Nogueira 17313-814 Porto de Goiás / RN	71636-333
55	Ana Sophia	da Mota	pedro-henrique85@example.com	205.719.486-85	1997-05-16	Conjunto de Nascimento, 138 Marçola 26018227 Viana / RO	79292091
77	Sophie	da Mota	livia08@example.net	672.598.143-73	1997-04-13	Condomínio Isabelly Cunha, 30 Prado 78943472 Rezende / PR	53778703
8	Vitor Gabriel	Caldeira	csilva@example.net	853.624.091-15	1973-07-11	Jardim da Luz, 96 Rio Branco 42420423 da Mata / MT	66925-534
49	Pietra	Caldeira	gustavo-henriquecastro@example.com	725.083.916-03	1977-08-06	Travessa Laura Monteiro, 7 Grota 92672446 Porto / AL	36084-897
61	Davi Lucca	Carvalho	vcavalcanti@example.net	526.984.013-89	1963-04-27	Recanto Nina da Costa Vila Santa Monica 1ª Seção 23645052 Castro de Fernandes / MA	03745319
58	Ana Julia	Ferreira	lorena19@example.com	945.218.603-89	1994-02-22	Vale Rezende, 4 São Geraldo 84460438 da Conceição de Sales / TO	33395-564
75	Maria Fernanda	Ferreira	vitor-hugoribeiro@example.net	283.076.541-90	1978-07-13	Ladeira de Nascimento, 359 Salgado Filho 60868-929 da Rosa / CE	01114-589
78	João Felipe	Ferreira	fernando53@example.org	369.428.710-87	1976-09-12	Favela Barros, 28 Nazare 98329-395 Caldeira do Norte / GO	83982-409
51	Pedro Lucas	Silveira	kda-cunha@example.com	823.456.790-00	1970-12-24	Trecho de Duarte, 903 Leticia 22951015 Nascimento / RR	87312248
93	Yuri	Silveira	barbarapereira@example.net	395.814.270-23	1980-09-09	Campo de Silveira, 36 Bernadete 75409639 Barbosa / RN	48093646
87	Luna	Teixeira	samuel34@example.org	403.762.985-29	1998-04-06	Viaduto Cunha, 3 Minas Brasil 94652281 Nascimento do Oeste / SC	53544065
73	Lucas Gabriel	da Costa	sophie99@example.net	749.536.012-70	1977-03-08	Conjunto Melo, 64 Vila Nova Gameleira 3ª Seção 99419458 Duarte Alegre / AC	80265890
34	Daniela	Fernandes	rezendemarcela@example.org	627.401.395-43	1996-07-22	Feira de Melo, 95 Bom Jesus 02499-885 Jesus / PE	73390-636
59	Eloah	Fernandes	ida-costa@example.org	486.321.970-96	1987-09-10	Recanto Duarte Vila Mangueiras 08038-110 Araújo / RO	73404-769
60	Sabrina	Fernandes	pedro82@example.net	643.508.219-70	1993-09-18	Conjunto Sofia Azevedo, 44 Ipe 31209677 Costela de Araújo / MA	24300799
15	Joana	Rodrigues	erodrigues@example.net	480.756.921-01	1968-05-21	Esplanada de Almeida, 720 Manacas 89334474 Cardoso de Cunha / MT	92472266
66	Lívia	Rodrigues	estherdias@example.com	953.124.067-16	1992-02-27	Praça Novaes, 93 Sion 55659988 Souza dos Dourados / SP	73034259
12	Ana Lívia	das Neves	milena37@example.net	182.045.369-33	1986-09-23	Vale Marcos Vinicius Duarte, 302 Vila Paris 03560827 Moura / PR	12951-292
70	Paulo	Cavalcanti	silveiraalicia@example.net	805.124.379-04	1966-08-18	Rodovia Igor Moura, 41 Atila De Paiva 22036-616 Pereira / SC	79567620
95	Juliana	Cavalcanti	gomesmanuela@example.org	309.764.251-06	1982-05-18	Trevo de Duarte Nossa Senhora Do Rosário 41440886 Rezende / MS	52427-851
94	Melissa	Gonçalves	caldeirasamuel@example.com	825.670.139-03	1971-01-09	Largo de Pinto, 96 Vila Independencia 2ª Seção 38053880 da Mota do Amparo / TO	66939-843
28	Alexandre	Nascimento	olivia97@example.org	719.360.824-03	1992-01-12	Aeroporto Maria Cecília Rocha, 39 Serra Do Curral 08025-854 Cardoso da Praia / MA	42178150
80	Clarice	Nascimento	lorena74@example.org	956.104.283-51	1995-12-01	Rua de da Mota, 7 Vila Mantiqueira 17429735 Jesus / SC	40016475
27	Marcela	da Conceição	mcosta@example.com	139.467.025-70	1963-05-14	Avenida de Campos, 513 Tres Marias 67624232 Correia / AM	66423556
35	Ryan	da Conceição	giovannacavalcanti@example.net	342.506.918-70	1989-11-03	Estação de Rezende, 33 Vila Betânia 65202-821 Silva / PI	59741896
\.


--
-- Data for Name: contas; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.contas (num_conta, cod_cliente, cod_agencia, cod_colaborador, tipo_conta, data_abertura, saldo_total, saldo_disponivel, data_ultimo_lancamento) FROM stdin;
53	53	1	7	PF	2011-05-24 12:30:00-03	2984.76	2814.67	2019-07-02 08:03:46-03
190	190	1	7	PF	2014-10-17 11:48:00-03	4262.43	4191.86	2021-03-25 09:00:15-03
191	191	1	7	PF	2013-03-23 11:14:00-03	3266.57	3080.71	2016-09-09 20:34:15-03
203	203	1	7	PF	2016-08-03 13:13:00-03	21241.54	19461.56	2022-12-28 04:58:24-03
254	254	1	7	PF	2012-10-24 11:48:00-02	13335.70	13197.54	2013-12-20 20:20:00-02
306	306	1	7	PF	2012-05-23 09:11:00-03	22997.47	21677.88	2016-10-05 00:14:36-03
472	472	1	7	PF	2022-08-07 09:08:00-03	4178.42	4183.68	2022-12-29 21:00:00.67516-03
520	520	1	7	PF	2019-11-03 13:26:00-03	1809.81	1288.51	2022-12-29 20:59:59.753932-03
739	739	1	7	PF	2014-02-26 09:20:00-03	45.77	616.79	2022-12-29 20:59:59.789361-03
509	509	1	13	PF	2012-01-20 09:56:00-02	19159.80	19863.76	2016-06-09 19:25:04-03
688	688	1	13	PF	2017-12-05 09:03:00-02	61105.56	55917.15	2022-09-24 10:18:37-03
775	775	1	13	PF	2011-03-19 11:55:00-03	39008.68	35288.34	2021-06-25 11:12:03-03
920	920	1	13	PF	2021-01-22 10:48:00-03	2023.58	2511.30	2022-12-29 20:59:59.953426-03
954	954	1	13	PF	2012-04-15 07:23:00-03	35545.03	34545.62	2020-03-21 23:59:42-03
955	955	1	13	PF	2020-03-28 11:02:00-03	79502.07	80952.72	2022-06-04 04:26:23-03
170	170	1	14	PF	2022-12-15 13:01:00-03	926.71	335.38	2023-01-11 06:04:20.2749-03
218	218	1	14	PF	2019-10-01 13:05:00-03	180027.69	168407.88	2022-09-09 07:51:55-03
555	555	1	14	PF	2012-08-24 09:57:00-03	52691.54	54240.69	2019-04-01 18:41:58-03
741	741	1	14	PF	2012-03-03 11:17:00-03	91849.87	83480.04	2020-11-17 19:35:58-03
751	751	1	14	PF	2012-01-13 10:29:00-02	59.75	44.86	2022-07-17 14:25:28-03
953	953	1	14	PF	2015-06-11 12:05:00-03	1866.73	1827.75	2022-12-29 20:59:59.106764-03
108	108	1	23	PF	2017-04-13 09:21:00-03	1171.45	812.56	2022-12-29 20:59:59.646511-03
110	110	1	23	PF	2020-06-19 13:06:00-03	11059.76	10208.05	2022-12-29 21:00:00.199407-03
242	242	1	23	PF	2010-05-06 11:03:00-03	165102.06	163409.68	2019-08-09 20:42:11-03
844	844	1	23	PF	2022-09-19 09:48:00-03	20803.94	20373.14	2022-12-29 20:59:59.639046-03
888	888	1	23	PF	2016-04-13 08:01:00-03	91067.68	87609.82	2021-05-22 19:18:09-03
171	171	1	24	PF	2022-03-04 13:24:00-03	68520.06	66507.14	2022-12-29 21:00:00.7855-03
291	291	1	24	PF	2012-02-26 09:34:00-03	40641.12	41016.20	2022-12-08 05:47:13-03
481	481	1	24	PF	2020-04-14 13:19:00-03	86.28	74.53	2022-11-20 13:50:25-03
564	564	1	24	PF	2015-05-20 08:34:00-03	20584.69	19955.30	2017-07-18 08:54:24-03
584	584	1	24	PF	2014-11-07 09:59:00-02	1658.17	1592.70	2019-08-27 09:10:09-03
635	635	1	24	PF	2022-09-12 12:38:00-03	1534.56	1252.55	2022-12-29 21:00:00.33746-03
116	116	1	30	PF	2014-10-17 10:19:00-03	20992.18	20458.98	2022-08-12 09:32:52-03
153	153	1	30	PF	2014-09-12 13:45:00-03	14581.90	14679.19	2019-05-05 11:56:38-03
176	176	1	30	PF	2019-04-20 11:06:00-03	14244.93	12934.41	2020-12-19 10:15:50-03
199	199	1	30	PF	2019-01-10 10:24:00-02	92338.31	94200.58	2022-12-29 20:37:56-03
207	207	1	30	PF	2022-06-24 13:07:00-03	13029.24	52.13	2022-12-29 21:00:00.621813-03
275	275	1	30	PF	2020-05-02 09:18:00-03	51431.95	52418.24	2022-12-29 21:00:00.087069-03
361	361	1	30	PF	2018-01-08 12:30:00-02	16828.83	17483.74	2022-09-28 20:25:00-03
701	701	1	30	PF	2013-02-24 07:46:00-03	53.50	37.57	2020-03-30 05:05:11-03
808	808	1	30	PF	2013-08-08 11:43:00-03	33978.83	32308.15	2022-04-21 15:11:48-03
910	910	1	30	PF	2011-11-03 14:42:00-02	38140.18	38894.44	2018-11-01 20:32:33-03
290	290	1	31	PF	2016-02-19 10:30:00-02	10809.29	8075.48	2022-12-29 20:59:59.668206-03
451	451	1	31	PF	2011-08-25 08:33:00-03	14880.73	15408.80	2013-08-31 20:35:07-03
557	557	1	31	PF	2017-12-09 10:29:00-02	2841.34	2543.82	2022-12-29 20:56:57-03
617	617	1	31	PF	2016-02-17 09:37:00-02	28163.42	28107.84	2017-10-26 17:24:41-02
632	632	1	31	PF	2017-07-25 13:41:00-03	15022.80	14852.30	2022-12-29 20:59:59.732522-03
740	740	1	31	PF	2019-11-12 09:14:00-03	788.60	697.98	2022-12-29 20:50:54-03
134	134	1	34	PF	2013-06-11 08:41:00-03	19285.30	19263.65	2017-06-15 04:46:25-03
410	410	1	34	PF	2019-04-22 10:24:00-03	2612.79	2608.29	2022-12-29 21:00:00.675814-03
429	429	1	34	PF	2015-05-28 12:38:00-03	7184.02	5418.26	2022-12-29 21:00:00.433124-03
496	496	1	34	PF	2020-02-27 09:50:00-03	18.42	78.95	2022-12-29 21:00:00.869078-03
631	631	1	34	PF	2010-12-10 13:06:00-02	21223.45	22164.99	2014-05-25 12:37:54-03
878	878	1	34	PF	2012-05-16 11:13:00-03	32869.20	34449.17	2021-05-13 17:48:23-03
6	6	1	42	PF	2010-03-09 09:09:00-03	14236.27	13407.48	2016-10-02 19:42:28-03
620	620	1	42	PF	2010-06-21 13:42:00-03	29814.95	28876.55	2014-01-29 17:49:43-02
714	714	1	42	PF	2015-06-18 08:20:00-03	48216.74	43787.36	2020-07-12 01:06:16-03
877	877	1	42	PF	2013-02-11 09:31:00-02	125716.28	126887.95	2019-05-21 17:49:47-03
286	286	1	58	PF	2021-07-12 09:27:00-03	3985.90	2427.58	2022-12-29 20:52:25-03
559	559	1	58	PF	2011-02-05 12:40:00-02	31372.24	29533.51	2022-12-29 21:00:00.049372-03
911	911	1	58	PF	2011-09-22 09:17:00-03	3545.66	3563.20	2022-12-29 20:59:59.851741-03
40	40	1	62	PF	2015-04-14 11:50:00-03	461.35	164.31	2022-12-29 20:59:59.352027-03
90	90	1	62	PF	2014-05-15 10:06:00-03	109727.88	99717.27	2015-12-25 19:14:30-02
93	93	1	62	PF	2021-10-27 08:09:00-03	354.41	270.18	2022-12-29 20:59:59.036565-03
147	147	1	62	PF	2014-08-14 11:32:00-03	42405.95	41516.75	2015-11-13 15:37:41-02
486	486	1	62	PF	2015-04-23 10:18:00-03	197.36	155.72	2022-12-29 19:47:31-03
501	501	1	62	PF	2018-06-20 11:51:00-03	232815.79	211636.04	2022-12-29 21:00:00.468941-03
670	670	1	62	PF	2022-10-25 11:11:00-03	42711.76	41331.12	2022-12-29 21:00:00.718266-03
735	735	1	62	PF	2012-12-03 11:05:00-02	72474.23	67091.37	2021-04-25 20:19:48-03
805	805	1	62	PF	2011-05-08 11:35:00-03	4438.54	4510.78	2022-12-29 21:00:00.828982-03
19	19	1	67	PF	2019-10-05 10:24:00-03	2615.32	2364.85	2022-12-29 20:59:59.115282-03
106	106	1	67	PF	2010-08-14 12:55:00-03	68269.51	71851.76	2020-09-28 17:33:20-03
146	146	1	67	PF	2010-06-27 07:33:00-03	10975.19	11304.70	2014-03-23 04:15:00-03
154	154	1	67	PF	2013-10-14 08:46:00-03	81928.25	78745.64	2018-05-10 15:30:26-03
198	198	1	67	PF	2016-09-09 08:33:00-03	7893.02	7802.34	2022-12-29 19:42:22-03
200	200	1	67	PF	2015-05-17 07:24:00-03	3503.06	2941.50	2022-12-26 04:35:44-03
260	260	1	67	PF	2013-10-22 12:33:00-02	23435.39	22294.71	2022-06-06 14:43:44-03
347	347	1	67	PF	2011-02-03 14:40:00-02	83141.24	87482.30	2015-05-01 01:58:44-03
357	357	1	67	PF	2021-07-12 12:17:00-03	71.39	21.40	2022-12-29 21:00:00.925889-03
497	497	1	67	PF	2021-10-25 13:18:00-03	44788.25	45546.60	2022-12-29 20:59:59.201726-03
598	598	1	67	PF	2015-02-09 11:15:00-02	6940.73	6722.36	2022-12-29 21:00:00.545758-03
715	715	1	67	PF	2018-12-20 14:17:00-02	27057.15	23267.77	2022-12-29 21:00:00.415793-03
925	925	1	67	PF	2014-07-10 11:52:00-03	7854.30	7813.65	2022-12-29 19:10:15-03
68	68	1	72	PF	2019-08-11 08:02:00-03	66.69	54.72	2022-12-29 20:59:59.607906-03
195	195	1	72	PF	2011-03-08 10:26:00-03	28846.58	27711.57	2015-04-03 08:22:27-03
280	280	1	72	PF	2017-04-20 11:07:00-03	14666.52	13614.04	2022-09-20 17:09:54-03
284	284	1	72	PF	2020-02-18 10:51:00-03	42994.12	43554.03	2022-12-29 19:10:49-03
604	604	1	72	PF	2022-03-20 13:32:00-03	66821.99	68837.85	2022-12-29 20:59:59.290722-03
697	697	1	72	PF	2013-01-24 10:46:00-02	1323.40	1185.81	2016-09-14 09:33:25-03
831	831	1	72	PF	2016-01-22 09:28:00-02	32572.58	31390.16	2022-12-29 20:57:40-03
141	141	1	77	PF	2012-05-10 10:49:00-03	8239.80	8311.67	2015-02-24 21:30:47-03
208	208	1	77	PF	2013-05-12 10:48:00-03	99593.47	101572.38	2016-07-29 12:49:35-03
382	382	1	77	PF	2010-02-02 12:28:00-02	6992.42	6935.26	2017-02-22 09:18:12-03
431	431	1	77	PF	2013-11-20 09:12:00-02	31323.33	31597.66	2019-04-02 09:38:44-03
574	574	1	77	PF	2014-03-05 13:10:00-03	11612.55	10535.63	2022-07-03 20:00:19-03
684	684	1	77	PF	2020-03-14 08:55:00-03	14568.50	14689.17	2022-12-29 21:00:00.317437-03
717	717	1	77	PF	2015-02-21 10:48:00-02	30082.00	29273.68	2020-02-20 23:00:45-03
892	892	1	77	PF	2015-08-05 09:19:00-03	8397.36	7660.12	2019-05-25 12:09:19-03
145	145	1	79	PF	2011-03-03 10:41:00-03	69766.79	65137.83	2020-10-03 11:06:51-03
152	152	1	79	PF	2017-07-01 11:56:00-03	68.44	16.22	2022-12-29 20:59:59.386084-03
258	258	1	79	PF	2011-08-21 11:49:00-03	14823.21	14252.92	2022-05-06 09:23:37-03
293	293	1	79	PF	2018-10-14 09:48:00-03	34326.80	33578.19	2022-10-28 23:01:59-03
473	473	1	79	PF	2016-01-02 08:15:00-02	50851.23	49224.08	2020-10-04 06:33:03-03
567	567	1	79	PF	2011-05-16 07:17:00-03	31479.60	29103.95	2015-09-15 08:33:09-03
758	758	1	79	PF	2014-12-25 11:17:00-02	62228.87	56551.49	2019-09-19 00:25:22-03
875	875	1	79	PF	2015-01-18 13:07:00-02	33365.15	30866.63	2022-12-29 20:55:10-03
989	989	1	79	PF	2021-11-25 11:47:00-03	754.61	577.40	2022-12-29 20:59:55-03
119	119	1	83	PF	2020-01-14 07:20:00-03	27043.28	26165.99	2022-10-26 03:36:27-03
186	186	1	83	PF	2022-02-21 12:05:00-03	164.71	134.20	2022-12-29 21:00:00.56732-03
231	231	1	83	PF	2012-06-25 08:43:00-03	52989.00	49969.30	2017-02-09 09:55:11-02
232	232	1	83	PF	2013-11-27 14:57:00-02	117243.30	110336.89	2020-07-24 05:10:00-03
243	243	1	83	PF	2017-04-01 11:23:00-03	15640.93	16447.33	2022-10-24 10:33:00-03
331	331	1	83	PF	2012-09-22 13:29:00-03	15569.42	15030.97	2022-12-29 20:59:59.268198-03
602	602	1	83	PF	2018-10-24 10:07:00-03	10115.97	9667.71	2021-02-19 01:34:16-03
671	671	1	83	PF	2021-08-09 13:15:00-03	8075.95	7551.61	2022-12-29 21:00:00.918448-03
804	804	1	83	PF	2011-12-06 11:00:00-02	58103.96	56919.71	2016-11-04 09:52:28-02
927	927	1	83	PF	2011-08-15 11:48:00-03	34239.26	33034.49	2013-02-06 11:38:57-02
938	938	1	83	PF	2016-12-12 10:27:00-02	167.21	116.36	2022-12-29 20:59:59.245727-03
42	42	2	12	PF	2013-04-12 09:10:00-03	27615.72	27609.02	2022-01-19 01:26:03-03
185	185	2	12	PF	2011-03-23 11:06:00-03	403.05	393.62	2021-03-04 16:53:30-03
415	415	2	12	PF	2015-06-09 11:03:00-03	66837.71	64200.64	2022-12-29 21:00:00.397829-03
504	504	2	12	PF	2015-04-22 11:18:00-03	45917.36	46241.17	2022-12-29 19:39:57-03
548	548	2	12	PF	2021-05-24 09:19:00-03	51462.00	47060.44	2022-12-29 20:59:59.281558-03
23	23	2	15	PF	2012-12-06 08:38:00-02	19605.44	19299.27	2022-12-29 20:58:10-03
55	55	2	15	PF	2016-11-10 12:25:00-02	353.76	322.26	2022-12-29 21:00:00.379315-03
85	85	2	15	PF	2010-10-04 10:19:00-03	55114.87	10769.58	2022-12-11 06:49:24-03
138	138	2	15	PF	2018-04-06 07:08:00-03	230.13	116.05	2022-12-29 20:59:59.356948-03
188	188	2	15	PF	2022-11-12 11:02:00-03	10111.06	9159.69	2022-12-29 21:00:00.476412-03
432	432	2	15	PF	2011-04-27 09:38:00-03	122319.38	115048.84	2022-12-29 20:59:59.49324-03
455	455	2	15	PF	2017-08-24 07:26:00-03	1440.95	1268.50	2020-11-08 14:27:02-03
517	517	2	15	PF	2020-12-11 12:57:00-03	62271.82	64049.89	2022-12-29 20:59:59.977786-03
545	545	2	15	PF	2019-12-16 07:22:00-03	20510.22	20524.46	2022-12-29 20:59:59.94329-03
621	621	2	15	PF	2015-03-11 08:29:00-03	52509.98	52048.08	2022-10-11 22:15:42-03
762	762	2	15	PF	2022-09-15 11:36:00-03	28104.60	27974.60	2022-12-29 20:59:59.911482-03
168	168	2	22	PF	2012-12-20 14:11:00-02	35959.30	37827.96	2022-07-06 11:23:09-03
456	456	2	22	PF	2014-02-12 13:54:00-02	25.76	12.04	2022-04-04 16:19:19-03
505	505	2	22	PF	2013-07-27 10:47:00-03	209128.93	191520.60	2022-10-19 17:01:15-03
566	566	2	22	PF	2013-11-11 10:53:00-02	5737.69	5160.79	2018-03-26 12:13:17-03
666	666	2	22	PF	2015-07-27 12:03:00-03	47389.75	48308.97	2020-02-25 08:30:30-03
750	750	2	22	PF	2012-05-05 08:32:00-03	3848.69	3727.02	2022-12-25 13:58:54-03
768	768	2	22	PF	2019-04-22 11:34:00-03	44919.13	42951.12	2022-12-29 20:55:59-03
14	14	2	28	PF	2014-08-26 11:14:00-03	3923.41	3491.48	2022-12-29 21:00:00.698664-03
45	45	2	28	PF	2015-10-19 12:51:00-02	15400.83	13450.80	2021-06-09 21:38:31-03
221	221	2	28	PF	2016-08-16 10:42:00-03	2058.98	2677.19	2022-12-28 15:28:44-03
534	534	2	28	PF	2015-01-25 14:56:00-02	32217.00	33477.58	2022-12-29 19:53:27-03
799	799	2	28	PF	2021-05-24 08:29:00-03	128.33	7.34	2022-12-29 21:00:00.146195-03
841	841	2	28	PF	2020-04-16 13:11:00-03	35333.46	36788.90	2022-12-28 20:54:25-03
896	896	2	28	PF	2018-06-19 11:13:00-03	36968.31	39530.31	2022-12-29 20:59:59.122591-03
915	915	2	28	PF	2018-12-25 10:03:00-02	12410.46	12940.12	2022-10-01 05:38:58-03
945	945	2	28	PF	2016-05-10 10:56:00-03	48981.15	49096.21	2022-12-03 11:04:20-03
435	435	2	46	PF	2011-02-02 09:14:00-02	41480.04	40881.44	2019-06-30 17:38:23-03
616	616	2	46	PF	2017-08-04 07:47:00-03	16991.02	15969.62	2022-12-29 20:59:59.913155-03
661	661	2	46	PF	2013-09-11 10:40:00-03	19.33	5.35	2022-12-29 20:59:56-03
664	664	2	46	PF	2017-11-12 13:21:00-02	17796.98	17333.02	2022-12-29 21:00:00.689084-03
667	667	2	46	PF	2022-03-06 12:06:00-03	987.98	760.39	2022-12-29 21:00:00.112756-03
766	766	2	46	PF	2014-09-07 10:15:00-03	271.74	334.31	2022-12-29 20:59:59.641-03
821	821	2	46	PF	2012-03-12 09:31:00-03	38099.70	37024.01	2016-02-26 11:02:42-03
36	36	2	56	PF	2012-02-17 13:58:00-02	41929.75	40313.21	2022-12-29 20:59:54-03
46	46	2	56	PF	2011-06-04 08:10:00-03	47538.62	151644.40	2016-09-08 03:26:30-03
277	277	2	56	PF	2013-06-09 09:38:00-03	1408.38	1263.39	2017-10-07 03:48:39-03
375	375	2	56	PF	2019-06-22 12:56:00-03	13593.81	12657.90	2022-12-29 20:59:59.227118-03
495	495	2	56	PF	2015-07-02 08:07:00-03	29290.94	28153.49	2022-12-29 21:00:00.823395-03
539	539	2	56	PF	2011-04-21 07:58:00-03	17405.56	16455.88	2015-09-25 08:33:15-03
744	744	2	56	PF	2012-03-21 08:46:00-03	4711.03	5493.63	2021-10-01 15:38:52-03
774	774	2	56	PF	2018-07-11 11:52:00-03	206361.40	196440.97	2022-12-29 20:56:56-03
903	903	2	56	PF	2020-08-16 08:05:00-03	83936.28	84439.77	2022-03-13 16:41:29-03
930	930	2	56	PF	2018-04-03 08:06:00-03	16311.09	8241.09	2022-12-18 09:54:26-03
62	62	2	57	PF	2010-12-02 10:14:00-02	6662.32	6390.18	2020-06-28 18:08:18-03
73	73	2	57	PF	2012-06-21 11:25:00-03	2612.72	2439.85	2017-05-21 06:14:01-03
94	94	2	57	PF	2011-09-09 12:00:00-03	34545.96	33166.61	2022-12-29 21:00:00.138357-03
175	175	2	57	PF	2015-12-17 12:44:00-02	1931.37	1744.86	2018-05-18 18:30:15-03
333	333	2	57	PF	2019-12-03 10:34:00-03	398.66	263.32	2022-12-29 20:59:59.673776-03
677	677	2	57	PF	2018-01-15 09:14:00-02	39346.53	38525.41	2022-07-12 14:02:07-03
760	760	2	57	PF	2013-04-27 07:08:00-03	20116.40	20875.57	2015-06-05 09:29:59-03
801	801	2	57	PF	2019-09-27 12:29:00-03	34040.21	34583.48	2022-12-29 20:59:59.136427-03
905	905	2	57	PF	2013-03-04 09:38:00-03	18219.17	17267.95	2016-08-05 20:19:11-03
990	990	2	57	PF	2020-01-23 13:28:00-03	6317.25	6536.31	2021-09-25 04:50:51-03
26	26	2	63	PF	2012-08-12 13:34:00-03	10388.78	10032.94	2021-02-13 01:38:56-03
51	51	2	63	PF	2022-01-18 11:52:00-03	330.55	305.04	2022-12-29 20:59:59.330763-03
140	140	2	63	PF	2015-02-10 11:47:00-02	140807.68	117253.54	2022-12-29 20:59:59.367528-03
235	235	2	63	PF	2018-06-01 10:05:00-03	114315.51	109905.99	2021-09-09 02:16:59-03
383	383	2	63	PF	2017-01-24 11:00:00-02	38945.74	40234.88	2018-06-24 00:58:10-03
633	633	2	63	PF	2011-05-08 10:51:00-03	16.80	0.60	2018-08-30 00:58:32-03
4	4	2	65	PF	2017-04-06 09:28:00-03	184007.86	170283.25	2022-11-12 08:50:05-03
27	27	2	65	PF	2022-05-10 09:29:00-03	263.73	228.68	2022-12-29 21:00:00.767904-03
95	95	2	65	PF	2021-09-22 12:00:00-03	176.31	253.21	2022-12-29 20:59:59.772328-03
11	11	2	69	PF	2010-11-12 14:37:00-02	8749.81	8414.48	2022-12-29 20:59:56-03
177	177	2	69	PF	2022-10-15 13:31:00-03	12129.61	12439.49	2022-12-29 20:59:59.932063-03
244	244	2	69	PF	2012-04-20 10:36:00-03	22751.99	22071.15	2022-12-23 23:19:53-03
251	251	2	69	PF	2013-07-11 11:28:00-03	8907.74	8346.63	2021-09-05 01:36:19-03
468	468	2	69	PF	2021-08-27 10:19:00-03	10007.90	10124.27	2022-12-29 21:00:00.468061-03
533	533	2	69	PF	2021-08-09 09:39:00-03	26541.77	24783.65	2022-12-29 20:59:59.604072-03
759	759	2	69	PF	2022-01-02 11:41:00-03	937.58	1702.40	2022-12-29 20:59:59.09932-03
919	919	2	69	PF	2014-10-18 09:17:00-03	11549.64	11770.39	2018-09-03 11:38:01-03
972	972	2	69	PF	2010-08-10 12:41:00-03	18882.36	17130.82	2022-05-07 22:59:15-03
166	166	2	70	PF	2014-11-21 13:42:00-02	124745.70	122476.89	2021-01-20 08:41:39-03
222	222	2	70	PF	2014-05-11 07:03:00-03	10214.64	10068.50	2017-07-29 21:08:27-03
642	642	2	70	PF	2019-06-26 09:58:00-03	14609.90	14856.86	2022-12-29 20:59:59.449488-03
655	655	2	70	PF	2016-02-11 12:22:00-02	74550.79	70325.65	2020-05-15 13:28:49-03
699	699	2	70	PF	2011-03-22 11:49:00-03	24351.57	22299.44	2022-12-29 20:59:59.406374-03
700	700	2	70	PF	2017-12-06 14:13:00-02	24760.97	25235.92	2022-12-29 21:00:00.050689-03
510	510	2	81	PF	2012-03-16 10:03:00-03	23050.59	20939.23	2022-12-29 20:59:59.194889-03
640	640	2	81	PF	2020-06-11 11:58:00-03	151.11	61.30	2022-12-29 20:59:59.961749-03
668	668	2	81	PF	2015-03-03 09:16:00-03	2967.72	918.58	2022-12-29 21:00:00.758288-03
879	879	2	81	PF	2013-07-12 07:44:00-03	238990.88	231759.98	2019-09-11 02:47:27-03
891	891	2	81	PF	2012-02-20 11:42:00-02	5171.98	5112.55	2018-08-12 05:36:11-03
913	913	2	81	PF	2021-10-08 11:19:00-03	297.75	128.35	2022-12-29 21:00:00.41435-03
75	75	2	96	PF	2011-06-12 10:50:00-03	43845.30	44058.34	2022-10-08 03:10:17-03
201	201	2	96	PF	2021-11-12 13:43:00-03	20524.26	19820.47	2022-12-29 21:00:00.097381-03
323	323	2	96	PF	2015-06-14 13:51:00-03	3914.17	3800.27	2022-12-29 21:00:00.568614-03
372	372	2	96	PF	2013-11-27 11:03:00-02	25957.56	24630.60	2019-10-25 05:58:32-03
425	425	2	96	PF	2021-01-22 10:13:00-03	773.54	29312.93	2022-12-29 20:59:59.637724-03
547	547	2	96	PF	2013-04-04 12:11:00-03	24560.00	45922.63	2018-01-29 21:27:08-02
623	623	2	96	PF	2018-06-12 08:47:00-03	274.86	195.38	2021-11-04 02:58:29-03
659	659	2	96	PF	2011-09-22 11:33:00-03	43301.92	40067.48	2019-10-04 07:28:51-03
797	797	2	96	PF	2018-10-21 11:14:00-03	66.32	40.06	2022-12-29 21:00:00.98649-03
826	826	2	96	PF	2017-07-16 07:31:00-03	3343.12	3376.43	2019-08-18 14:54:24-03
890	890	2	96	PF	2013-12-11 12:23:00-02	91.91	34.61	2022-09-19 04:33:48-03
998	998	2	96	PF	2013-11-07 11:05:00-02	12110.95	47398.84	2016-07-24 09:03:50-03
135	135	3	4	PF	2016-07-03 09:30:00-03	7367.60	7665.52	2020-11-27 08:01:16-03
189	189	3	4	PF	2021-09-16 08:47:00-03	3657.94	2875.48	2022-12-29 20:47:10-03
656	656	3	4	PF	2019-02-20 09:24:00-03	30350.78	30770.78	2021-05-18 01:02:16-03
814	814	3	4	PF	2014-09-09 11:55:00-03	20261.34	19025.32	2018-09-10 00:08:32-03
876	876	3	4	PF	2019-04-17 08:01:00-03	5137.50	5069.37	2022-12-29 20:59:59.711504-03
895	895	3	4	PF	2022-08-15 08:35:00-03	3191.86	3020.27	2022-12-29 21:00:00.708347-03
963	963	3	4	PF	2022-06-13 12:07:00-03	6909.48	6250.55	2022-12-29 20:59:59.305111-03
70	70	3	27	PF	2021-02-18 12:23:00-03	108.51	28940.35	2022-12-29 21:00:00.135754-03
229	229	3	27	PF	2013-01-20 09:26:00-02	15665.47	15511.60	2016-11-13 17:53:00-02
368	368	3	27	PF	2016-03-03 10:31:00-03	12017.39	12075.63	2022-12-29 20:59:59.147739-03
376	376	3	27	PF	2012-04-07 12:08:00-03	11668.15	10745.11	2022-09-30 05:42:18-03
721	721	3	27	PF	2022-07-17 11:30:00-03	1729.32	1623.71	2022-12-29 20:59:59.060207-03
845	845	3	27	PF	2020-12-16 12:14:00-03	904.80	477.95	2022-12-29 21:00:00.802509-03
897	897	3	27	PF	2016-12-07 09:46:00-02	208.51	156.15	2022-12-29 20:59:59.625723-03
21	21	3	37	PF	2016-07-10 09:29:00-03	7250.57	6957.31	2022-11-24 16:30:54-03
129	129	3	37	PF	2013-01-08 13:31:00-02	40602.26	38336.69	2020-04-09 04:00:43-03
212	212	3	37	PF	2021-04-09 12:40:00-03	26933.38	26472.64	2022-12-29 20:59:59.48936-03
420	420	3	37	PF	2013-01-24 08:10:00-02	10852.62	11170.85	2017-05-22 17:51:50-03
608	608	3	37	PF	2014-09-20 08:49:00-03	28555.18	28544.94	2019-12-14 14:57:10-03
767	767	3	37	PF	2019-12-06 12:52:00-03	77102.73	74114.26	2022-12-18 11:40:10-03
850	850	3	37	PF	2013-07-22 11:42:00-03	117660.80	107914.04	2018-08-04 22:57:43-03
173	173	3	41	PF	2021-05-12 11:45:00-03	87.44	536.72	2022-12-29 20:59:59.464383-03
525	525	3	41	PF	2012-05-03 10:10:00-03	22773.81	20567.89	2017-10-21 05:21:47-02
576	576	3	41	PF	2015-12-26 11:07:00-02	1618.48	1368.46	2021-04-11 21:08:06-03
582	582	3	41	PF	2013-02-21 08:58:00-03	36087.81	37148.30	2022-12-29 21:00:00.462063-03
603	603	3	41	PF	2018-04-20 07:44:00-03	4323.15	4352.13	2022-12-29 12:20:07-03
689	689	3	41	PF	2014-04-15 08:08:00-03	37557.59	34398.15	2022-12-29 19:51:57-03
861	861	3	41	PF	2018-07-15 08:31:00-03	3621.64	3264.96	2022-09-26 13:33:15-03
104	104	3	45	PF	2018-01-04 10:09:00-02	10225.38	10009.55	2022-12-29 21:00:00.624769-03
179	179	3	45	PF	2019-01-15 09:29:00-02	67454.05	66086.48	2022-12-29 20:59:59.901142-03
364	364	3	45	PF	2013-09-07 11:06:00-03	19006.90	19977.87	2019-02-24 02:44:55-03
371	371	3	45	PF	2012-12-04 10:48:00-02	35852.78	32782.46	2022-12-29 20:58:41-03
535	535	3	45	PF	2014-09-06 10:52:00-03	35.27	24.18	2021-09-06 15:08:48-03
800	800	3	45	PF	2015-02-25 12:05:00-03	45523.67	46356.45	2022-12-29 21:00:00.482356-03
840	840	3	45	PF	2013-02-05 09:08:00-02	18951.81	19926.05	2022-12-29 20:59:59.777193-03
966	966	3	45	PF	2013-11-05 13:33:00-02	15348.31	15243.72	2022-12-29 05:21:46-03
71	71	3	47	PF	2013-12-14 11:59:00-02	16164.54	14842.35	2019-12-04 00:28:17-03
162	162	3	47	PF	2012-11-15 09:37:00-02	22674.02	22376.76	2016-11-24 08:37:37-02
238	238	3	47	PF	2019-03-08 13:08:00-03	786.88	783.20	2022-12-29 20:59:59.181535-03
374	374	3	47	PF	2013-07-28 08:52:00-03	71434.55	69922.48	2021-03-04 18:21:16-03
430	430	3	47	PF	2013-08-16 08:02:00-03	1546.67	1451.68	2019-03-11 01:35:56-03
752	752	3	47	PF	2017-10-28 13:26:00-02	657.92	538.65	2022-12-29 20:59:59.381012-03
778	778	3	47	PF	2018-12-21 12:46:00-02	4988.43	5373.21	2022-12-29 20:59:59.562888-03
832	832	3	47	PF	2012-12-25 13:27:00-02	56012.79	51659.17	2019-09-23 19:31:07-03
863	863	3	47	PF	2015-11-26 10:41:00-02	118.81	56.56	2018-09-23 18:05:00-03
3	3	3	49	PF	2013-01-05 11:13:00-02	25244.49	24079.04	2022-08-14 17:51:30-03
155	155	3	49	PF	2013-08-22 10:27:00-03	57577.01	57526.38	2018-09-21 19:25:28-03
523	523	3	49	PF	2013-03-18 07:44:00-03	16025.31	14722.03	2015-06-04 15:42:29-03
765	765	3	49	PF	2019-05-27 09:48:00-03	291.08	200.49	2022-12-29 20:59:59.267491-03
846	846	3	49	PF	2013-10-12 07:33:00-03	143847.09	130458.18	2022-12-29 21:00:00.110692-03
881	881	3	49	PF	2019-12-23 12:28:00-03	5707.79	5466.55	2022-12-29 20:57:20-03
886	886	3	49	PF	2014-04-26 12:43:00-03	3586.69	3645.79	2019-03-02 01:00:38-03
909	909	3	74	PF	2019-01-04 11:26:00-02	64506.15	67700.81	2022-11-25 08:40:32-03
56	56	3	78	PF	2022-09-21 11:40:00-03	149048.22	139067.40	2022-12-29 20:59:59.172722-03
269	269	3	78	PF	2013-01-05 11:51:00-02	15245.68	14424.13	2020-06-26 11:50:37-03
325	325	3	78	PF	2014-04-14 12:34:00-03	23586.85	24763.26	2017-06-18 02:59:33-03
480	480	3	78	PF	2012-12-09 09:58:00-02	104.98	11.27	2022-11-03 12:02:20-03
507	507	3	78	PF	2015-04-20 08:35:00-03	296.15	254.43	2021-09-24 08:34:05-03
599	599	3	78	PF	2020-01-11 11:52:00-03	161.94	139.21	2022-12-27 02:16:01-03
898	898	3	78	PF	2013-02-25 12:54:00-03	79839.16	79023.89	2018-03-08 23:47:00-03
914	914	3	78	PF	2017-11-11 09:08:00-02	675.83	650.86	2022-12-26 00:19:05-03
43	43	3	88	PF	2016-09-03 12:08:00-03	43388.05	39429.83	2022-12-29 20:59:59.486255-03
80	80	3	88	PF	2015-04-23 12:08:00-03	28432.93	29330.00	2022-12-29 20:58:24-03
205	205	3	88	PF	2016-01-15 09:42:00-02	28440.02	26009.76	2022-08-23 14:11:54-03
373	373	3	88	PF	2013-07-19 12:18:00-03	47371.69	48268.47	2016-11-07 21:16:26-02
419	419	3	88	PF	2013-05-01 12:18:00-03	1444.26	1153.85	2022-12-29 21:00:00.898288-03
115	115	3	98	PF	2013-01-16 13:15:00-02	6.47	82556.41	2016-08-13 21:37:46-03
193	193	3	98	PF	2014-06-24 11:39:00-03	9466.42	9226.57	2022-12-29 20:59:59.974984-03
248	248	3	98	PF	2019-06-13 08:34:00-03	919.25	871.17	2022-08-07 14:26:08-03
298	298	3	98	PF	2012-04-21 12:53:00-03	106461.80	99486.45	2019-04-06 01:06:26-03
408	408	3	98	PF	2021-10-15 07:47:00-03	9193.06	8497.31	2022-12-29 21:00:00.614611-03
416	416	3	98	PF	2022-12-18 10:37:00-03	35643.51	34907.80	2023-01-01 17:22:28.291488-03
592	592	3	98	PF	2014-10-28 12:46:00-02	493.41	427.08	2022-12-06 15:19:31-03
866	866	3	98	PF	2019-06-22 10:36:00-03	4.34	624.82	2022-12-29 21:00:00.276554-03
906	906	3	98	PF	2012-11-25 12:07:00-02	25095.49	25725.94	2022-12-29 20:59:59.463588-03
960	960	3	98	PF	2014-07-05 10:14:00-03	78171.21	77835.11	2020-01-09 21:14:06-03
9	9	4	3	PF	2022-12-08 08:21:00-03	81101.63	75739.07	2022-12-30 13:23:13.549956-03
39	39	4	3	PF	2022-04-25 12:35:00-03	12353.21	12760.23	2022-12-29 20:59:59.468678-03
460	460	4	3	PF	2014-03-04 07:43:00-03	4130.98	2982.80	2022-12-24 01:32:38-03
571	571	4	3	PF	2018-01-18 12:36:00-02	469.75	316.68	2022-12-29 21:00:00.108166-03
580	580	4	3	PF	2014-05-04 10:13:00-03	17823.12	17114.17	2022-12-29 20:59:59.585665-03
727	727	4	3	PF	2020-08-10 09:55:00-03	2660.26	1976.68	2022-06-08 07:09:04-03
780	780	4	3	PF	2018-02-05 09:35:00-02	2476.53	2414.31	2022-02-20 04:42:18-03
813	813	4	3	PF	2020-08-28 08:27:00-03	410.84	202.07	2022-12-29 21:00:00.084096-03
848	848	4	3	PF	2022-10-14 11:02:00-03	6.09	26.09	2022-12-29 20:59:59.524731-03
924	924	4	3	PF	2021-10-20 12:55:00-03	26185.64	23748.10	2022-12-29 20:59:14-03
943	943	4	3	PF	2017-09-07 12:09:00-03	35595.45	32521.38	2022-12-29 21:00:00.929314-03
5	5	4	32	PF	2015-01-14 09:53:00-02	137649.19	120169.19	2018-07-24 00:18:52-03
49	49	4	32	PF	2021-05-05 09:58:00-03	349.16	97.49	2022-12-29 20:59:59.235451-03
148	148	4	32	PF	2016-02-22 08:31:00-03	6451.98	5824.07	2022-12-28 16:44:36-03
187	187	4	32	PF	2022-03-08 12:48:00-03	8302.29	7893.98	2022-12-29 20:59:59.691567-03
250	250	4	32	PF	2018-12-08 11:43:00-02	8020.77	9242.54	2022-12-29 20:59:59.833297-03
279	279	4	32	PF	2018-04-09 08:15:00-03	21046.98	21497.92	2022-12-29 20:59:59.844831-03
356	356	4	32	PF	2014-01-13 09:30:00-02	44776.38	40667.30	2022-06-01 11:56:38-03
444	444	4	32	PF	2022-11-20 13:56:00-03	3825.86	3737.08	2022-12-29 21:00:00.907773-03
467	467	4	32	PF	2018-02-26 09:52:00-03	32232.68	32091.99	2022-12-29 20:59:59.242815-03
615	615	4	32	PF	2021-06-11 13:02:00-03	98888.47	90717.11	2022-12-29 21:00:00.184665-03
638	638	4	32	PF	2018-07-05 08:41:00-03	314.00	19.70	2022-12-29 20:59:59.306088-03
705	705	4	32	PF	2015-07-24 10:19:00-03	7272.48	7395.50	2019-05-21 02:28:33-03
749	749	4	32	PF	2018-08-23 09:31:00-03	45291.67	43485.14	2021-05-18 12:03:23-03
781	781	4	32	PF	2014-09-23 08:27:00-03	337.70	78.42	2022-07-26 12:01:24-03
828	828	4	32	PF	2021-12-25 09:52:00-03	10753.21	10954.17	2022-12-29 21:00:00.363526-03
830	830	4	32	PF	2018-01-11 12:07:00-02	20823.54	19957.63	2022-12-29 13:43:16-03
834	834	4	32	PF	2015-03-25 11:02:00-03	241.17	179.65	2018-08-31 00:04:19-03
923	923	4	32	PF	2021-05-05 10:11:00-03	21432.62	21785.72	2022-12-29 20:59:59-03
988	988	4	32	PF	2021-02-22 11:02:00-03	2195.14	2052.75	2022-12-28 18:18:58-03
57	57	4	61	PF	2017-05-04 13:51:00-03	698.57	619.98	2022-12-29 20:58:01-03
60	60	4	61	PF	2017-11-28 12:52:00-02	3646.37	3704.51	2022-02-17 14:18:46-03
161	161	4	61	PF	2015-08-17 12:19:00-03	7354.64	6742.02	2021-10-24 10:08:04-03
230	230	4	61	PF	2020-12-22 13:37:00-03	8796.05	8269.42	2022-12-29 21:00:00.169379-03
252	252	4	61	PF	2019-07-18 09:56:00-03	269.71	237.12	2021-06-17 10:59:18-03
322	322	4	61	PF	2014-06-27 12:40:00-03	48224.93	48608.93	2019-02-24 20:20:51-03
350	350	4	61	PF	2018-11-15 13:42:00-02	65716.30	66372.28	2022-12-29 21:00:00.14673-03
541	541	4	61	PF	2019-04-25 10:04:00-03	80038.67	82151.29	2022-12-29 15:39:24-03
568	568	4	61	PF	2020-08-22 12:21:00-03	78357.28	74084.74	2022-12-29 20:59:59.094036-03
575	575	4	61	PF	2015-07-27 08:15:00-03	80964.10	79474.92	2022-06-23 04:35:02-03
852	852	4	61	PF	2015-07-01 13:05:00-03	41988.59	42369.58	2016-09-28 12:36:43-03
970	970	4	61	PF	2021-01-07 12:42:00-03	34423.44	32777.22	2022-12-29 06:00:54-03
164	164	4	73	PF	2014-09-08 09:52:00-03	6258.36	6164.68	2020-05-21 00:00:44-03
233	233	4	73	PF	2014-12-02 12:14:00-02	30.06	9.76	2019-04-08 22:26:15-03
259	259	4	73	PF	2019-10-12 09:17:00-03	143.99	102.77	2022-12-29 20:59:59.925936-03
304	304	4	73	PF	2019-07-04 08:12:00-03	47188.65	44492.25	2021-03-07 10:57:27-03
346	346	4	73	PF	2019-03-02 08:07:00-03	72228.95	66842.31	2022-06-21 19:38:31-03
363	363	4	73	PF	2022-05-03 08:19:00-03	5784.43	4613.95	2022-12-29 20:59:59.540603-03
366	366	4	73	PF	2014-06-06 12:19:00-03	18102.16	18358.39	2022-12-29 20:59:59.693692-03
390	390	4	73	PF	2018-08-17 12:11:00-03	245.62	301.42	2022-12-20 13:53:47-03
448	448	4	73	PF	2019-12-18 10:11:00-03	108585.12	108046.82	2022-12-29 21:00:00.302617-03
515	515	4	73	PF	2014-04-17 12:32:00-03	127.81	101.63	2022-12-29 21:00:00.819885-03
614	614	4	73	PF	2014-04-16 10:23:00-03	17250.89	17057.35	2018-10-11 10:45:20-03
624	624	4	73	PF	2015-08-03 13:55:00-03	6263.42	5759.04	2022-04-09 13:15:28-03
703	703	4	73	PF	2015-03-04 10:12:00-03	13916.08	14255.22	2020-04-08 14:50:08-03
723	723	4	73	PF	2013-12-02 12:38:00-02	29617.02	27618.22	2019-08-17 00:45:19-03
753	753	4	73	PF	2017-07-04 12:05:00-03	13400.35	13211.55	2022-09-08 05:44:46-03
776	776	4	73	PF	2021-03-14 12:58:00-03	117.60	76.58	2022-12-29 20:59:59.00265-03
798	798	4	73	PF	2015-11-21 08:49:00-02	539.24	509.95	2022-12-29 20:56:27-03
838	838	4	73	PF	2013-11-15 14:17:00-02	20058.30	19265.34	2021-05-08 04:11:47-03
899	899	4	73	PF	2021-10-17 10:00:00-03	8539.37	8294.29	2022-12-29 20:59:59.35822-03
934	934	4	73	PF	2017-08-25 13:02:00-03	4177.67	3450.27	2022-12-29 20:59:59.465741-03
940	940	4	73	PF	2016-10-25 09:53:00-02	7979.81	8178.80	2022-12-29 20:55:32-03
974	974	4	73	PF	2014-07-18 12:15:00-03	1633.75	1166.87	2016-11-23 18:48:56-02
993	993	4	73	PF	2017-10-07 08:40:00-03	6775.41	6150.55	2021-11-16 22:08:31-03
102	102	5	5	PF	2015-02-28 13:28:00-03	15506.31	14875.64	2022-12-29 20:59:59.841585-03
158	158	5	5	PF	2018-10-22 12:47:00-03	102.45	60.28	2022-02-07 02:58:52-03
312	312	5	5	PF	2022-07-12 13:34:00-03	2990.55	2843.45	2022-12-29 20:59:59.301049-03
532	532	5	5	PF	2021-07-13 11:56:00-03	49694.78	49628.38	2022-12-29 21:00:00.473273-03
747	747	5	5	PF	2022-11-18 11:04:00-03	505.39	507.20	2022-12-29 20:59:59.735921-03
180	180	5	9	PF	2022-07-21 09:36:00-03	3046.32	3004.33	2022-12-29 20:59:59.643909-03
215	215	5	9	PF	2020-04-10 11:14:00-03	45569.01	44209.51	2022-12-29 20:59:59.137126-03
413	413	5	9	PF	2014-05-20 08:41:00-03	12999.93	13377.54	2022-12-29 20:10:57-03
600	600	5	9	PF	2021-01-10 12:35:00-03	2724.05	2350.11	2022-12-29 20:59:59.635068-03
980	980	5	9	PF	2022-07-27 08:26:00-03	5490.92	5758.24	2022-12-29 21:00:00.431605-03
1	1	5	10	PF	2014-07-08 13:25:00-03	22473.87	22538.06	2022-11-08 10:22:46-03
223	223	5	10	PF	2022-12-17 13:43:00-03	30.26	12.23	2022-12-29 20:59:59.024565-03
476	476	5	10	PF	2018-05-26 12:11:00-03	37413.60	34814.88	2021-02-23 03:10:46-03
630	630	5	10	PF	2014-07-02 09:56:00-03	799.29	698.33	2022-12-03 07:49:57-03
833	833	5	10	PF	2014-07-17 09:29:00-03	16409.37	15424.99	2021-03-23 00:37:51-03
157	157	5	21	PF	2017-08-20 09:13:00-03	183979.04	172773.53	2022-12-27 05:19:50-03
326	326	5	21	PF	2018-01-27 12:50:00-02	35921.45	1284.29	2022-09-02 08:18:09-03
447	447	5	21	PF	2022-03-08 12:28:00-03	453.50	396.53	2022-12-29 20:59:59.104941-03
514	514	5	21	PF	2021-01-20 08:35:00-03	12237.53	11325.96	2022-12-29 20:59:59.719657-03
196	196	5	36	PF	2014-06-05 09:16:00-03	34585.58	35985.86	2021-04-27 11:55:59-03
343	343	5	36	PF	2021-09-20 09:12:00-03	25938.22	24644.54	2022-12-29 21:00:00.725948-03
692	692	5	36	PF	2014-07-11 09:48:00-03	34962.18	32166.28	2022-12-29 21:00:00.681571-03
987	987	5	36	PF	2014-08-02 11:57:00-03	2148.68	2159.69	2016-08-28 22:18:41-03
328	328	5	43	PF	2017-03-04 10:03:00-03	36948.71	33788.96	2019-05-02 02:36:42-03
562	562	5	43	PF	2021-07-16 13:50:00-03	1973.93	1811.49	2022-12-29 20:59:59.876057-03
653	653	5	43	PF	2016-06-16 11:20:00-03	94097.65	86317.93	2022-12-29 20:59:59.734108-03
817	817	5	43	PF	2021-11-25 12:41:00-03	41127.40	41896.73	2022-12-29 20:59:59.306737-03
309	309	5	50	PF	2015-07-15 08:21:00-03	14816.41	15334.76	2022-12-29 20:36:48-03
516	516	5	50	PF	2019-09-19 12:25:00-03	335.99	349.48	2022-12-29 21:00:00.409004-03
613	613	5	50	PF	2017-10-24 11:13:00-02	18773.49	18763.11	2022-05-13 00:12:29-03
810	810	5	50	PF	2017-07-07 12:17:00-03	635.03	918.39	2022-12-29 20:59:49-03
939	939	5	50	PF	2022-12-13 10:16:00-03	822.83	809.35	2022-12-29 21:00:00.151225-03
975	975	5	50	PF	2021-03-18 11:58:00-03	20.86	46.71	2022-12-29 21:00:00.328832-03
192	192	5	52	PF	2015-09-27 08:21:00-03	92644.50	96197.79	2022-12-29 21:00:00.752528-03
450	450	5	52	PF	2019-04-01 09:40:00-03	18485.22	17763.99	2022-11-30 16:32:43-03
611	611	5	52	PF	2019-08-06 11:42:00-03	35913.29	33542.41	2022-12-29 11:27:05-03
725	725	5	52	PF	2020-01-11 11:13:00-03	116.37	20.83	2022-12-29 20:59:59.912766-03
651	651	5	55	PF	2016-07-14 09:22:00-03	43605.49	44909.55	2021-08-23 09:42:40-03
779	779	5	55	PF	2014-04-15 08:46:00-03	3915.34	4232.93	2021-03-27 05:36:14-03
837	837	5	55	PF	2014-03-18 07:30:00-03	18181.17	16654.64	2020-05-26 19:47:20-03
327	327	5	59	PF	2021-10-08 08:35:00-03	12913.30	12434.70	2022-12-29 20:59:59.770314-03
388	388	5	59	PF	2017-02-14 10:43:00-02	14348.66	13919.02	2022-12-24 11:10:53-03
462	462	5	59	PF	2016-11-19 11:53:00-02	400.81	340.18	2019-12-29 05:22:09-03
485	485	5	59	PF	2017-09-23 08:16:00-03	21129.43	19292.72	2022-07-07 03:04:30-03
786	786	5	59	PF	2019-01-21 12:29:00-02	25708.99	24386.97	2022-12-29 21:00:00.091285-03
790	790	5	59	PF	2014-07-03 13:49:00-03	8696.47	8079.57	2021-09-20 20:50:12-03
862	862	5	59	PF	2017-11-07 09:35:00-02	57375.23	54060.36	2022-12-25 11:04:09-03
926	926	5	59	PF	2018-05-03 12:18:00-03	58479.78	53607.32	2022-12-27 03:03:23-03
370	370	5	60	PF	2022-07-08 10:14:00-03	370.37	308.99	2022-12-29 20:59:59.788932-03
484	484	5	60	PF	2021-07-01 09:51:00-03	158638.33	148919.10	2022-12-29 20:59:59.668365-03
676	676	5	60	PF	2016-04-25 12:44:00-03	7591.33	6859.66	2018-10-13 09:34:22-03
820	820	5	60	PF	2017-05-04 09:48:00-03	4803.10	4043.63	2022-12-29 20:59:59-03
237	237	5	76	PF	2021-07-28 08:45:00-03	230.83	197.93	2022-12-29 20:59:59.348435-03
340	340	5	76	PF	2022-02-15 09:44:00-03	68771.11	65285.17	2022-12-29 20:59:59.219102-03
29	29	5	92	PF	2015-07-27 08:08:00-03	33.40	26.34	2022-12-29 21:00:00.324619-03
72	72	5	92	PF	2019-04-07 12:08:00-03	27508.06	24529.73	2022-12-29 20:59:59.764744-03
76	76	5	92	PF	2015-11-06 13:36:00-02	330.34	294.93	2022-12-29 20:59:59.429706-03
127	127	5	92	PF	2014-04-15 07:06:00-03	111063.22	109931.60	2017-12-18 19:50:39-02
234	234	5	92	PF	2021-03-08 10:43:00-03	2649.29	2269.76	2022-12-29 21:00:00.97399-03
387	387	5	92	PF	2014-02-08 11:28:00-02	47010.33	43828.91	2022-12-29 21:00:00.360945-03
597	597	5	92	PF	2019-06-19 09:21:00-03	106127.15	108338.43	2022-12-29 20:59:59.568438-03
405	405	6	2	PF	2021-11-17 12:03:00-03	429.43	275.21	2022-12-29 21:00:00.962089-03
440	440	6	2	PF	2022-03-02 08:10:00-03	1333.13	1062.26	2022-12-29 20:59:59.640396-03
612	612	6	2	PF	2017-02-05 10:35:00-02	12974.97	13553.67	2022-12-29 20:59:59.409272-03
870	870	6	2	PF	2017-12-20 09:12:00-02	51333.78	48084.30	2022-12-13 15:36:48-03
2	2	6	16	PF	2020-03-27 10:05:00-03	59.28	12.72	2022-12-29 21:00:00.071193-03
33	33	6	16	PF	2016-11-10 11:52:00-02	11565.98	10887.28	2022-12-29 21:00:00.750304-03
64	64	6	16	PF	2016-04-18 12:57:00-03	6246.97	6501.51	2022-02-07 05:57:18-03
427	427	6	16	PF	2019-02-18 11:37:00-03	8085.96	7760.45	2022-03-04 12:00:08-03
641	641	6	16	PF	2019-12-07 10:32:00-03	1.31	744.27	2022-12-29 20:59:59.205176-03
856	856	6	16	PF	2015-06-24 12:02:00-03	11790.66	10831.83	2022-12-29 20:40:08-03
889	889	6	16	PF	2022-11-20 11:49:00-03	828.41	786.36	2022-12-29 21:00:00.463347-03
24	24	6	17	PF	2018-06-10 08:18:00-03	35486.75	34541.16	2022-12-29 21:00:00.324887-03
172	172	6	17	PF	2020-06-20 12:24:00-03	24099.87	24708.18	2022-12-29 20:54:14-03
332	332	6	17	PF	2020-09-10 08:54:00-03	85186.37	83239.81	2022-12-29 21:00:00.136318-03
578	578	6	17	PF	2021-02-03 11:52:00-03	19177.22	18603.70	2022-12-29 21:00:00.50693-03
819	819	6	17	PF	2022-08-02 08:24:00-03	54632.65	56789.22	2022-12-29 21:00:00.774919-03
163	163	6	29	PF	2020-07-26 11:45:00-03	16598.96	15220.77	2022-12-29 20:59:59.547366-03
169	169	6	29	PF	2015-07-01 10:19:00-03	25114.20	25729.27	2022-10-24 02:07:23-03
365	365	6	29	PF	2020-03-11 10:52:00-03	341020.82	333783.57	2022-12-29 10:45:03-03
449	449	6	29	PF	2017-11-10 10:04:00-02	532.30	438.38	2022-12-29 20:59:59.762018-03
706	706	6	29	PF	2019-01-10 13:36:00-02	1624.15	1231.68	2022-12-29 21:00:00.678695-03
935	935	6	29	PF	2018-03-22 12:23:00-03	327.51	186.16	2022-12-29 21:00:00.640178-03
958	958	6	29	PF	2018-10-09 12:05:00-03	10828.38	8596.76	2022-12-21 21:14:33-03
763	763	6	44	PF	2016-10-19 12:58:00-02	2213.52	2061.36	2022-12-27 19:47:18-03
107	107	6	53	PF	2020-04-19 09:09:00-03	273.68	160.81	2022-09-08 19:01:13-03
211	211	6	53	PF	2021-03-10 09:41:00-03	40789.45	41996.88	2022-12-29 21:00:00.75253-03
518	518	6	53	PF	2015-10-03 11:31:00-03	9879.86	9275.92	2022-12-29 20:59:50-03
785	785	6	53	PF	2021-03-15 12:01:00-03	666.87	524.20	2022-12-29 21:00:00-03
48	48	6	54	PF	2019-03-09 13:19:00-03	2545.29	2366.42	2022-12-29 20:59:59-03
452	452	6	54	PF	2020-07-18 08:22:00-03	159474.24	146962.43	2022-12-29 20:59:59.577003-03
643	643	6	54	PF	2022-03-26 07:46:00-03	32.82	37.67	2022-12-29 20:59:59.901664-03
381	381	6	66	PF	2016-10-21 13:50:00-02	14217.10	12694.71	2022-12-05 16:35:29-03
513	513	6	66	PF	2021-07-01 08:43:00-03	13393.13	13010.52	2022-12-29 21:00:00.909153-03
742	742	6	66	PF	2019-05-17 12:36:00-03	1612.41	1498.33	2022-11-30 19:26:57-03
206	206	6	87	PF	2018-01-05 14:28:00-02	19807.66	19957.40	2022-06-29 20:00:58-03
217	217	6	87	PF	2018-09-05 11:44:00-03	1918.21	1807.49	2022-12-29 21:00:00.245138-03
316	316	6	87	PF	2018-01-23 13:29:00-02	29713.00	30107.64	2022-12-29 20:59:59.123017-03
377	377	6	87	PF	2016-12-15 13:58:00-02	16675.08	15678.09	2022-12-29 20:59:59.570142-03
610	610	6	87	PF	2020-11-01 11:28:00-03	1658.48	1592.91	2022-12-29 20:59:59.05012-03
733	733	6	87	PF	2020-09-26 07:37:00-03	340.40	253.59	2022-06-01 11:56:47-03
932	932	6	87	PF	2015-06-21 09:06:00-03	11546.25	11282.89	2022-12-29 20:59:59.867681-03
249	249	6	93	PF	2019-01-10 12:25:00-02	715.50	659.20	2022-12-16 18:22:34-03
386	386	6	93	PF	2018-11-03 09:58:00-03	39716.58	36353.97	2020-12-29 10:32:54-03
590	590	6	93	PF	2018-10-09 08:33:00-03	33128.77	29602.17	2022-12-29 20:59:59.140697-03
720	720	6	93	PF	2021-03-23 07:56:00-03	4125.00	3729.58	2022-12-29 20:59:59.313784-03
871	871	6	93	PF	2022-08-22 11:00:00-03	93.76	31.70	2022-12-29 20:59:59.070331-03
956	956	6	93	PF	2017-07-11 12:45:00-03	1352.76	842.29	2022-12-29 20:59:59.244944-03
35	35	6	97	PF	2015-09-11 09:06:00-03	47696.28	44817.87	2022-09-02 22:15:46-03
109	109	6	97	PF	2022-03-16 10:53:00-03	35.22	21.66	2022-12-29 21:00:00.141983-03
255	255	6	97	PF	2022-10-10 07:25:00-03	9955.43	9653.13	2022-12-29 21:00:00.544877-03
320	320	6	97	PF	2017-08-26 10:14:00-03	37.83	19.21	2022-12-29 21:00:00.752502-03
540	540	6	97	PF	2018-12-07 14:22:00-02	19180.56	18481.90	2021-08-19 01:14:57-03
15	15	7	20	PF	2022-09-11 12:51:00-03	2647.60	3218.29	2022-12-29 21:00:00.679061-03
16	16	7	20	PF	2018-04-20 09:01:00-03	2065.38	2132.85	2022-12-29 20:35:48-03
17	17	7	20	PF	2020-03-12 09:51:00-03	326.41	237.60	2022-12-29 20:59:59.64537-03
22	22	7	20	PF	2020-11-14 08:11:00-03	161.32	103.16	2022-12-29 21:00:00.984558-03
31	31	7	20	PF	2016-09-25 11:39:00-03	21553.25	20280.68	2022-05-07 19:35:02-03
52	52	7	20	PF	2022-08-14 12:20:00-03	22026.20	17655.11	2022-12-29 20:59:59.006537-03
59	59	7	20	PF	2017-03-10 09:19:00-03	796.55	764.36	2022-10-11 09:56:13-03
61	61	7	20	PF	2015-11-19 13:34:00-02	361.86	241.98	2022-12-29 20:30:58-03
74	74	7	20	PF	2016-07-26 07:37:00-03	125.97	47.88	2022-12-29 07:20:39-03
84	84	7	20	PF	2021-05-18 10:34:00-03	36568.16	33313.75	2022-12-28 15:00:21-03
92	92	7	20	PF	2017-02-03 13:31:00-02	4.74	96.52	2019-07-30 00:31:10-03
112	112	7	20	PF	2021-07-11 09:15:00-03	43416.76	42606.49	2022-12-29 21:00:00.403303-03
114	114	7	20	PF	2022-11-11 08:04:00-03	3760.20	17418.36	2022-12-29 21:00:00.855203-03
131	131	7	20	PF	2022-03-07 07:12:00-03	36.80	822.97	2022-12-29 20:59:59.668242-03
160	160	7	20	PF	2019-11-02 13:31:00-03	723.93	702.08	2022-12-29 21:00:00.272893-03
210	210	7	20	PF	2018-11-15 12:36:00-02	97.46	67.79	2022-12-29 16:24:01-03
220	220	7	20	PF	2017-08-17 08:51:00-03	400725.03	377919.46	2022-05-11 12:01:12-03
227	227	7	20	PF	2022-12-27 10:30:00-03	9071.87	8827.14	2022-12-29 21:00:00.577376-03
239	239	7	20	PF	2017-02-25 13:31:00-03	20825.14	21003.24	2022-09-24 22:03:58-03
241	241	7	20	PF	2021-02-26 09:19:00-03	7555.26	6136.91	2022-12-29 20:59:59.851594-03
265	265	7	20	PF	2019-06-05 08:17:00-03	36319.33	34565.51	2022-12-29 20:50:17-03
266	266	7	20	PF	2016-09-15 08:01:00-03	25309.07	23006.93	2021-10-10 09:19:52-03
270	270	7	20	PF	2019-09-05 13:38:00-03	22222.92	21620.27	2022-12-29 21:00:00.604683-03
285	285	7	20	PF	2017-08-06 10:36:00-03	13532.30	12467.51	2022-12-29 21:00:00.725196-03
295	295	7	20	PF	2017-07-13 10:27:00-03	11694.36	11614.69	2022-12-24 14:23:35-03
299	299	7	20	PF	2022-05-02 09:39:00-03	47.74	2.57	2022-12-29 21:00:00.280458-03
300	300	7	20	PF	2021-06-24 12:39:00-03	368.99	354.60	2022-12-29 20:59:59.045471-03
339	339	7	20	PF	2019-10-13 12:10:00-03	24710.69	22194.86	2022-12-29 20:59:59.253001-03
351	351	7	20	PF	2020-08-19 13:52:00-03	404.95	253.86	2022-12-29 20:59:59.487488-03
353	353	7	20	PF	2021-10-19 09:13:00-03	10744.80	10071.98	2022-12-29 20:59:59.642663-03
354	354	7	20	PF	2020-02-24 12:41:00-03	26258.02	24081.21	2022-12-29 20:59:59.550399-03
358	358	7	20	PF	2019-03-15 12:56:00-03	38618.04	36812.24	2022-12-29 21:00:00.67282-03
362	362	7	20	PF	2019-11-09 07:52:00-03	53389.32	51760.47	2022-12-29 21:00:00.027547-03
393	393	7	20	PF	2022-10-27 13:27:00-03	14967.48	13695.14	2022-12-29 21:00:00.953339-03
394	394	7	20	PF	2016-12-21 11:31:00-02	25574.68	25979.61	2022-09-04 11:21:39-03
399	399	7	20	PF	2018-09-23 13:32:00-03	30431.92	28423.39	2022-12-29 21:00:00.617979-03
402	402	7	20	PF	2021-07-15 09:44:00-03	57385.86	54610.49	2022-12-29 21:00:00.233882-03
424	424	7	20	PF	2022-02-10 09:31:00-03	96353.49	88192.70	2022-12-29 20:59:59.736308-03
434	434	7	20	PF	2021-08-02 12:38:00-03	324.06	945.62	2022-11-09 11:01:03-03
442	442	7	20	PF	2022-12-17 10:24:00-03	22957.82	20014.45	2023-01-15 12:57:23.427556-03
446	446	7	20	PF	2015-10-08 10:35:00-03	4383.79	3850.53	2022-03-14 19:14:51-03
492	492	7	20	PF	2016-06-17 08:48:00-03	262.46	239.80	2022-12-29 21:00:00.949544-03
499	499	7	20	PF	2022-11-07 08:37:00-03	135452.03	130201.51	2022-12-29 20:59:59.934531-03
531	531	7	20	PF	2020-04-17 10:12:00-03	922.64	579.95	2022-12-10 07:44:24-03
544	544	7	20	PF	2018-09-08 09:35:00-03	83742.51	82074.95	2022-07-01 11:59:44-03
553	553	7	20	PF	2017-10-25 10:12:00-02	27282.17	26909.57	2022-12-29 20:04:30-03
586	586	7	20	PF	2021-04-18 08:35:00-03	85580.49	84678.16	2022-12-29 20:59:59.71737-03
587	587	7	20	PF	2019-09-06 12:59:00-03	45734.28	21792.21	2022-12-29 20:59:59.766139-03
596	596	7	20	PF	2022-08-06 10:27:00-03	766.10	689.65	2022-12-29 20:59:59.965911-03
601	601	7	20	PF	2019-06-24 07:12:00-03	55381.31	50163.84	2022-12-29 20:59:58-03
626	626	7	20	PF	2022-07-25 07:12:00-03	27364.75	25670.85	2022-12-29 20:59:59.700666-03
634	634	7	20	PF	2021-07-02 10:31:00-03	48.63	21.57	2022-12-29 21:00:00.099734-03
662	662	7	20	PF	2022-12-15 09:50:00-03	25648.76	24995.14	2022-12-31 00:13:47.434674-03
678	678	7	20	PF	2016-06-06 09:17:00-03	9935.51	9839.71	2021-12-12 09:27:17-03
679	679	7	20	PF	2019-01-08 13:11:00-02	8394.09	8519.33	2022-06-30 07:11:49-03
704	704	7	20	PF	2021-10-11 11:10:00-03	16362.50	15714.77	2022-12-29 20:38:42-03
712	712	7	20	PF	2019-06-16 09:47:00-03	34828.10	45918.69	2022-12-29 20:56:56-03
718	718	7	20	PF	2018-03-09 11:52:00-03	44328.16	40732.59	2022-12-25 18:35:51-03
730	730	7	20	PF	2019-05-02 08:22:00-03	29.15	16.43	2022-12-29 20:57:11-03
773	773	7	20	PF	2018-02-27 13:20:00-03	4140.31	3896.88	2021-01-08 00:42:17-03
789	789	7	20	PF	2021-09-04 11:30:00-03	1840.62	932.73	2022-12-29 21:00:00.165285-03
829	829	7	20	PF	2020-04-16 07:33:00-03	256.76	106.62	2022-12-29 21:00:00.573423-03
842	842	7	20	PF	2020-10-02 07:21:00-03	1193.70	1109.88	2022-12-29 21:00:00.848782-03
868	868	7	20	PF	2022-12-02 10:06:00-03	443.11	407.87	2022-12-29 21:00:00.944789-03
887	887	7	20	PF	2021-02-27 08:43:00-03	17067.48	15458.46	2022-12-29 21:00:00.726093-03
900	900	7	20	PF	2021-08-06 09:29:00-03	375.72	71.92	2022-12-29 21:00:00.560008-03
952	952	7	20	PF	2019-12-04 09:14:00-03	1104.79	823.30	2022-12-21 01:27:09-03
977	977	7	20	PF	2018-12-15 11:03:00-02	1129.52	1012.65	2022-08-11 02:32:58-03
983	983	7	20	PF	2019-04-15 09:52:00-03	1415.92	1307.75	2022-12-29 21:00:00.902268-03
28	28	7	26	PF	2017-04-03 13:11:00-03	52654.36	50324.19	2019-11-24 03:23:48-03
65	65	7	26	PF	2018-08-26 09:33:00-03	255.26	208.15	2022-06-24 04:04:11-03
67	67	7	26	PF	2018-05-14 10:27:00-03	3343.62	3092.46	2022-08-25 20:00:41-03
78	78	7	26	PF	2019-12-27 09:52:00-03	5572.91	5007.53	2022-12-29 21:00:00.809693-03
81	81	7	26	PF	2021-04-03 10:18:00-03	4317.82	4035.66	2022-12-29 20:55:31-03
83	83	7	26	PF	2021-10-07 10:04:00-03	16.12	9550.96	2022-12-29 20:59:59.01211-03
86	86	7	26	PF	2016-03-05 10:55:00-03	140.70	180.20	2022-12-29 20:59:59.734403-03
113	113	7	26	PF	2020-01-07 11:29:00-03	647.69	30.71	2022-12-29 21:00:00.861205-03
118	118	7	26	PF	2022-04-14 09:33:00-03	265.88	244.56	2022-12-29 20:59:59.719978-03
144	144	7	26	PF	2022-04-06 11:46:00-03	347.14	885.10	2022-12-29 20:59:59.082654-03
183	183	7	26	PF	2019-12-19 11:01:00-03	1028.98	2655.28	2022-05-29 21:01:16-03
224	224	7	26	PF	2019-09-20 09:30:00-03	91.82	32.29	2022-12-25 05:00:32-03
245	245	7	26	PF	2018-06-15 08:38:00-03	27657.72	26339.26	2022-12-29 20:59:59-03
246	246	7	26	PF	2019-05-23 12:55:00-03	73563.28	74061.33	2022-12-29 20:59:59.627232-03
264	264	7	26	PF	2020-03-19 11:13:00-03	1065.24	946.80	2022-12-29 21:00:00.735424-03
274	274	7	26	PF	2018-08-16 09:55:00-03	105741.37	101639.81	2022-12-29 21:00:00.961613-03
276	276	7	26	PF	2021-02-03 08:25:00-03	10241.66	9370.81	2022-12-29 21:00:00-03
283	283	7	26	PF	2018-10-13 09:41:00-03	12870.35	12935.61	2020-02-17 17:36:45-03
301	301	7	26	PF	2015-10-20 11:23:00-02	23199.27	21653.35	2018-02-13 20:02:10-02
310	310	7	26	PF	2020-11-04 10:05:00-03	6616.23	6132.56	2022-12-29 20:59:59.317275-03
317	317	7	26	PF	2020-08-17 11:35:00-03	5980.91	5950.89	2022-12-29 21:00:00.052211-03
342	342	7	26	PF	2018-05-18 12:37:00-03	33695.25	33336.16	2022-12-27 18:36:43-03
360	360	7	26	PF	2021-08-15 08:39:00-03	22218.49	20525.77	2022-12-29 20:59:59.867839-03
385	385	7	26	PF	2018-08-06 09:05:00-03	12044.82	13802.43	2022-12-27 08:30:13-03
411	411	7	26	PF	2022-03-10 07:33:00-03	165559.71	158495.13	2022-12-29 20:59:59.944608-03
414	414	7	26	PF	2017-04-10 11:29:00-03	6141.77	5669.23	2020-11-10 00:12:37-03
417	417	7	26	PF	2022-10-17 10:56:00-03	11095.68	9982.92	2022-12-29 21:00:00.800093-03
421	421	7	26	PF	2020-04-24 07:07:00-03	466.62	439.10	2022-01-16 10:03:01-03
433	433	7	26	PF	2021-12-16 08:19:00-03	815.30	63.93	2022-12-29 21:00:00.875751-03
437	437	7	26	PF	2022-10-27 08:52:00-03	742.21	608.31	2022-12-29 20:59:59.69084-03
457	457	7	26	PF	2017-04-14 08:13:00-03	3056.27	2809.97	2022-10-24 03:20:14-03
464	464	7	26	PF	2021-04-20 07:09:00-03	11680.43	10722.87	2022-12-29 20:59:59.922199-03
477	477	7	26	PF	2022-08-20 11:19:00-03	1851.45	1311.39	2022-12-29 21:00:00.000358-03
491	491	7	26	PF	2020-09-03 11:33:00-03	34816.11	36237.31	2022-12-29 20:59:59.616802-03
498	498	7	26	PF	2015-08-25 11:46:00-03	5685.15	5739.17	2022-04-14 06:59:35-03
506	506	7	26	PF	2017-11-25 14:17:00-02	11257.52	11621.17	2022-12-28 18:37:30-03
521	521	7	26	PF	2022-09-13 10:05:00-03	248.55	146.28	2022-12-29 21:00:00.093542-03
527	527	7	26	PF	2015-08-10 09:14:00-03	151338.39	142774.92	2022-05-07 07:06:41-03
529	529	7	26	PF	2020-03-23 12:22:00-03	31630.65	29764.67	2022-12-29 21:00:00.420726-03
530	530	7	26	PF	2018-02-14 10:30:00-02	11555.85	11723.30	2020-10-08 15:48:41-03
550	550	7	26	PF	2017-06-25 12:12:00-03	102824.60	95089.16	2022-12-29 21:00:00.450168-03
552	552	7	26	PF	2018-06-17 11:15:00-03	31.29	769.68	2022-12-29 20:59:59.668211-03
573	573	7	26	PF	2021-05-11 12:05:00-03	830.56	498.36	2022-12-29 20:59:59.304535-03
579	579	7	26	PF	2018-10-03 13:00:00-03	7337.75	6816.73	2022-08-12 05:56:55-03
581	581	7	26	PF	2017-07-06 08:09:00-03	8627.61	7873.11	2020-12-16 04:28:46-03
588	588	7	26	PF	2019-09-07 12:19:00-03	6797.46	6144.82	2022-12-29 21:00:00.738262-03
622	622	7	26	PF	2018-01-24 12:45:00-02	2143.14	1912.43	2022-12-29 21:00:00-03
628	628	7	26	PF	2020-07-08 07:33:00-03	85151.42	84354.36	2022-12-29 21:00:00.579349-03
636	636	7	26	PF	2016-12-26 13:16:00-02	7746.56	7061.73	2020-04-17 13:03:00-03
637	637	7	26	PF	2017-03-05 13:46:00-03	3806.22	3668.45	2021-07-18 17:15:52-03
646	646	7	26	PF	2018-08-15 12:30:00-03	45815.84	44858.96	2022-12-29 20:59:51-03
649	649	7	26	PF	2016-04-11 10:35:00-03	21961.58	22871.31	2021-11-14 00:58:03-03
650	650	7	26	PF	2022-07-12 10:16:00-03	7.04	684.28	2022-12-29 21:00:00.047577-03
674	674	7	26	PF	2021-02-10 10:27:00-03	235.97	179.31	2022-12-29 20:59:59.238639-03
680	680	7	26	PF	2016-08-07 09:18:00-03	55.69	39.95	2022-10-19 14:49:07-03
681	681	7	26	PF	2018-07-23 10:19:00-03	160415.01	90516.74	2022-12-29 21:00:00.936138-03
690	690	7	26	PF	2022-02-27 11:01:00-03	27760.70	28603.20	2022-12-29 20:59:59.652586-03
691	691	7	26	PF	2020-07-15 13:46:00-03	107912.57	101404.81	2022-05-08 05:59:54-03
716	716	7	26	PF	2019-05-02 13:21:00-03	427.16	228.04	2022-12-29 20:59:39-03
728	728	7	26	PF	2020-11-23 09:34:00-03	19557.77	20119.83	2022-12-29 20:59:59.205189-03
737	737	7	26	PF	2021-01-28 11:26:00-03	901.60	2456.42	2022-12-29 20:59:59.840133-03
761	761	7	26	PF	2016-06-07 12:04:00-03	9.27	1232.10	2022-12-29 21:00:00.119507-03
771	771	7	26	PF	2020-01-16 09:55:00-03	92428.74	97788.51	2022-12-29 20:59:59.687011-03
782	782	7	26	PF	2019-01-10 09:19:00-02	35279.74	32124.63	2022-02-19 04:16:19-03
783	783	7	26	PF	2015-09-21 08:35:00-03	41501.58	37538.71	2019-01-07 17:33:10-02
794	794	7	26	PF	2019-11-14 08:15:00-03	996.41	938.79	2022-12-29 20:56:29-03
807	807	7	26	PF	2022-07-28 13:18:00-03	50704.86	51891.35	2022-12-29 21:00:00.592089-03
836	836	7	26	PF	2022-08-23 08:20:00-03	419.59	370.66	2022-12-29 21:00:00.462972-03
849	849	7	26	PF	2021-06-05 08:43:00-03	5047.48	5126.29	2022-12-29 21:00:00.659239-03
864	864	7	26	PF	2017-04-28 12:30:00-03	12665.79	13352.69	2022-01-05 14:26:55-03
902	902	7	26	PF	2018-03-17 12:41:00-03	1374.98	1311.14	2022-12-29 20:33:50-03
912	912	7	26	PF	2020-07-16 09:22:00-03	23870.49	24429.25	2022-12-29 20:59:59.389408-03
941	941	7	26	PF	2022-05-22 09:02:00-03	272.64	47.30	2022-12-29 21:00:00.400207-03
946	946	7	26	PF	2016-03-05 07:07:00-03	379.39	239.80	2022-12-26 17:25:06-03
968	968	7	26	PF	2018-02-01 14:14:00-02	170.82	96.02	2022-12-29 21:00:00.714991-03
971	971	7	26	PF	2021-02-12 09:24:00-03	18364.65	16534.76	2022-12-29 21:00:00.373015-03
979	979	7	26	PF	2018-03-09 13:27:00-03	33568.81	30770.18	2022-10-19 16:23:43-03
981	981	7	26	PF	2021-09-13 11:38:00-03	824.66	328.66	2022-12-29 20:59:59.232516-03
982	982	7	26	PF	2019-11-25 08:04:00-03	92.57	29.60	2022-12-29 20:59:59.062577-03
984	984	7	26	PF	2016-12-15 10:04:00-02	9173.72	8208.76	2022-12-29 20:59:59.205493-03
991	991	7	26	PF	2019-02-14 11:36:00-02	14.08	3.53	2022-12-29 20:57:35-03
995	995	7	26	PF	2021-02-06 10:22:00-03	872.16	801.72	2022-12-29 21:00:00.165332-03
996	996	7	26	PF	2017-02-20 11:29:00-03	37556.42	34524.93	2022-12-29 20:59:59.192225-03
997	997	7	26	PF	2020-11-20 10:29:00-03	88.69	84.08	2022-12-29 20:59:59.374137-03
8	8	7	48	PF	2021-08-13 08:07:00-03	1238.66	1054.95	2022-12-29 21:00:00.045457-03
18	18	7	48	PF	2021-07-18 12:07:00-03	226548.63	217787.98	2022-12-29 21:00:00.494383-03
54	54	7	48	PF	2017-09-08 09:55:00-03	30239.92	30255.81	2022-12-29 20:59:59.04732-03
77	77	7	48	PF	2019-02-01 11:17:00-02	415.61	349.81	2022-12-29 15:22:35-03
87	87	7	48	PF	2020-07-08 11:09:00-03	35777.78	34063.46	2022-12-29 21:00:00.140185-03
97	97	7	48	PF	2021-09-28 11:36:00-03	46143.09	46508.29	2022-12-29 21:00:00.234617-03
105	105	7	48	PF	2018-11-27 13:02:00-02	34360.53	32440.67	2022-12-29 21:00:00.472328-03
124	124	7	48	PF	2021-03-27 11:20:00-03	2933.24	2862.07	2022-12-29 21:00:00.089132-03
130	130	7	48	PF	2019-11-15 07:31:00-03	487.23	729.13	2022-12-29 20:59:59.154984-03
139	139	7	48	PF	2016-09-23 11:05:00-03	437.81	401.65	2022-10-31 03:36:34-03
143	143	7	48	PF	2020-04-19 10:29:00-03	2958.60	2772.60	2022-12-29 21:00:00.1751-03
149	149	7	48	PF	2020-06-25 09:32:00-03	21312.00	19341.19	2022-12-29 20:59:59.32956-03
178	178	7	48	PF	2020-07-17 13:24:00-03	24518.92	23767.60	2022-12-29 20:59:59.861661-03
197	197	7	48	PF	2020-09-26 09:01:00-03	198787.40	181219.01	2022-12-29 20:59:59.717134-03
202	202	7	48	PF	2016-04-22 08:19:00-03	19693.78	17714.93	2022-12-29 20:59:59.959341-03
216	216	7	48	PF	2019-12-06 08:03:00-03	75578.50	74679.17	2022-12-29 21:00:00.805063-03
226	226	7	48	PF	2017-01-14 09:27:00-02	30179.97	27655.51	2022-12-29 20:59:59.118784-03
228	228	7	48	PF	2022-03-04 10:41:00-03	78851.94	82933.80	2022-12-29 20:59:59.82221-03
271	271	7	48	PF	2020-12-02 09:41:00-03	72928.57	71467.54	2022-12-29 20:59:59.892527-03
297	297	7	48	PF	2018-08-11 10:45:00-03	925.88	879.13	2020-03-20 02:25:01-03
307	307	7	48	PF	2017-02-10 14:42:00-02	95.07	72.72	2022-12-29 20:59:50-03
319	319	7	48	PF	2021-08-18 12:41:00-03	62237.18	64114.92	2022-12-29 20:59:59.733003-03
341	341	7	48	PF	2016-04-12 09:25:00-03	9032.98	7596.00	2022-12-29 05:28:05-03
352	352	7	48	PF	2022-06-05 11:12:00-03	25597.94	24148.99	2022-12-29 20:59:59.631499-03
380	380	7	48	PF	2015-10-25 13:27:00-02	28308.88	29778.26	2022-12-29 19:26:11-03
389	389	7	48	PF	2021-12-13 10:02:00-03	101.15	98.44	2022-12-29 20:59:59.261739-03
398	398	7	48	PF	2022-02-16 11:30:00-03	87.64	71.32	2022-12-29 21:00:00.412707-03
418	418	7	48	PF	2016-05-12 09:48:00-03	26645.94	27003.54	2021-03-23 05:48:36-03
422	422	7	48	PF	2022-11-27 10:46:00-03	188.43	101.76	2022-12-29 20:59:59.967295-03
438	438	7	48	PF	2017-06-10 11:28:00-03	11894.96	10485.76	2022-11-27 07:34:00-03
459	459	7	48	PF	2019-11-05 07:33:00-03	124.79	74.03	2022-09-12 04:13:55-03
479	479	7	48	PF	2022-09-27 08:03:00-03	328.57	290.31	2022-12-29 21:00:00.247874-03
487	487	7	48	PF	2019-07-08 12:16:00-03	32798.23	30920.12	2022-12-29 20:59:59.421768-03
490	490	7	48	PF	2021-06-07 07:30:00-03	3991.06	3463.56	2022-12-29 20:59:59.395256-03
500	500	7	48	PF	2016-11-20 11:28:00-02	11679.79	10787.50	2018-03-16 21:50:02-03
537	537	7	48	PF	2020-02-02 13:09:00-03	6522.84	6180.00	2022-12-29 20:59:19-03
543	543	7	48	PF	2017-07-05 12:58:00-03	4106.56	3765.36	2022-12-29 20:59:59.928113-03
549	549	7	48	PF	2019-06-22 12:37:00-03	1106.77	609.15	2021-06-22 11:14:44-03
554	554	7	48	PF	2022-06-02 12:03:00-03	20.31	187.92	2022-12-29 20:59:59.858781-03
577	577	7	48	PF	2016-03-15 13:07:00-03	17829.87	16712.74	2020-01-24 03:19:25-03
606	606	7	48	PF	2021-06-18 11:14:00-03	1239.81	1161.93	2022-12-29 20:59:59.61666-03
625	625	7	48	PF	2021-01-09 08:36:00-03	10787.12	9793.87	2022-12-29 21:00:00.894665-03
647	647	7	48	PF	2018-12-19 11:08:00-02	143.43	118.65	2021-11-07 17:51:03-03
685	685	7	48	PF	2021-02-27 12:29:00-03	56057.11	53350.32	2022-12-29 20:55:37-03
687	687	7	48	PF	2021-12-14 07:04:00-03	44954.05	42763.73	2022-12-29 20:59:59.761148-03
698	698	7	48	PF	2018-08-14 12:16:00-03	52949.88	49952.45	2022-12-29 20:59:59.584934-03
710	710	7	48	PF	2019-06-26 09:44:00-03	3706.83	3069.41	2022-09-27 19:56:46-03
732	732	7	48	PF	2018-07-26 11:07:00-03	26142.37	26741.50	2022-12-29 19:57:44-03
755	755	7	48	PF	2019-05-27 10:47:00-03	48347.01	45095.63	2022-12-29 20:59:59.120822-03
756	756	7	48	PF	2021-01-16 09:06:00-03	11326.05	11697.77	2022-12-29 21:00:00.894387-03
769	769	7	48	PF	2017-01-07 12:53:00-02	3666.87	3430.07	2022-08-28 23:51:43-03
787	787	7	48	PF	2017-10-10 09:04:00-03	109334.01	103374.03	2022-12-29 20:59:59.897455-03
796	796	7	48	PF	2019-11-08 12:59:00-03	5281.92	5077.32	2022-11-30 05:27:05-03
802	802	7	48	PF	2015-11-05 09:08:00-02	32374.37	29643.65	2019-03-14 10:46:53-03
824	824	7	48	PF	2016-01-07 10:26:00-02	18454.71	19004.63	2017-05-11 00:33:48-03
839	839	7	48	PF	2017-01-02 10:37:00-02	14016.34	14495.29	2019-10-04 07:15:58-03
855	855	7	48	PF	2017-02-25 10:28:00-03	48777.11	44463.94	2022-12-29 20:59:59.12807-03
880	880	7	48	PF	2017-06-06 11:49:00-03	20335.09	18485.37	2022-12-29 21:00:00.847649-03
883	883	7	48	PF	2015-09-05 13:18:00-03	29382.13	28322.77	2017-05-01 08:52:54-03
937	937	7	48	PF	2022-06-16 08:01:00-03	1659.43	1094.97	2022-12-29 20:59:59.334982-03
967	967	7	48	PF	2016-02-21 11:33:00-03	62645.78	61092.73	2021-05-01 15:32:58-03
37	37	7	64	PF	2022-07-03 13:07:00-03	352.04	328.26	2022-12-29 21:00:00.548502-03
47	47	7	64	PF	2021-12-07 08:46:00-03	37349.64	35527.59	2022-12-29 20:59:59.13653-03
82	82	7	64	PF	2019-07-03 10:17:00-03	2813.93	2530.10	2021-01-31 12:14:12-03
89	89	7	64	PF	2021-08-26 10:23:00-03	69758.36	71055.33	2022-12-29 20:59:59.589585-03
103	103	7	64	PF	2018-05-25 10:51:00-03	38172.35	49975.53	2022-12-24 22:32:24-03
126	126	7	64	PF	2017-07-25 09:04:00-03	57292.05	52158.79	2022-05-19 18:41:47-03
142	142	7	64	PF	2021-10-04 12:56:00-03	229.99	370.30	2022-12-29 20:55:11-03
165	165	7	64	PF	2020-01-23 09:15:00-03	9170.93	8355.74	2022-12-29 21:00:00.817421-03
167	167	7	64	PF	2021-08-21 11:10:00-03	132218.81	121260.38	2022-12-29 20:59:59.968117-03
213	213	7	64	PF	2019-08-18 08:33:00-03	60366.02	55301.67	2022-12-29 21:00:00.30342-03
225	225	7	64	PF	2020-08-18 11:58:00-03	419923.18	411617.06	2022-12-29 21:00:00.269647-03
240	240	7	64	PF	2021-03-16 11:29:00-03	6642.56	6437.99	2022-12-29 20:59:59.51902-03
261	261	7	64	PF	2015-09-20 09:35:00-03	40119.08	38945.43	2022-12-29 20:44:39-03
263	263	7	64	PF	2017-12-27 11:22:00-02	14228.49	14502.43	2022-12-29 21:00:00.617224-03
267	267	7	64	PF	2022-09-12 08:33:00-03	15860.20	15184.19	2022-12-29 21:00:00.444154-03
289	289	7	64	PF	2022-12-11 11:34:00-03	392.81	392.94	2022-12-29 20:59:59.132011-03
292	292	7	64	PF	2015-10-10 09:03:00-03	134384.26	141742.22	2022-12-25 20:15:34-03
294	294	7	64	PF	2021-05-13 08:44:00-03	52858.19	53347.53	2022-12-29 21:00:00.992566-03
302	302	7	64	PF	2017-01-10 11:05:00-02	47083.90	43163.70	2021-11-16 11:44:15-03
303	303	7	64	PF	2017-04-11 10:13:00-03	59.48	31.21	2021-04-10 19:33:27-03
315	315	7	64	PF	2022-12-19 11:37:00-03	15067.78	14528.58	2022-12-29 20:59:59.317419-03
329	329	7	64	PF	2019-06-25 12:18:00-03	22775.06	18148.49	2022-12-29 21:00:00.84273-03
379	379	7	64	PF	2021-02-21 10:18:00-03	1110.26	873.60	2022-12-29 20:59:59.357548-03
412	412	7	64	PF	2020-12-01 11:13:00-03	299.25	233.56	2022-12-29 21:00:00.335954-03
423	423	7	64	PF	2017-11-26 14:12:00-02	7737.60	7639.31	2022-11-12 21:07:27-03
441	441	7	64	PF	2019-06-05 13:33:00-03	23947.12	24571.72	2022-12-29 21:00:00.018795-03
454	454	7	64	PF	2022-09-12 10:27:00-03	1612.04	1266.84	2022-12-29 20:59:59.804937-03
461	461	7	64	PF	2021-03-12 12:04:00-03	472.88	285.26	2022-12-29 20:59:59.315902-03
482	482	7	64	PF	2019-08-23 08:22:00-03	67872.97	67382.17	2022-12-26 15:18:07-03
538	538	7	64	PF	2022-11-25 08:24:00-03	2676.30	2429.12	2022-12-29 21:00:00.311419-03
542	542	7	64	PF	2022-06-02 08:54:00-03	275.73	249.04	2022-12-29 21:00:00.246974-03
546	546	7	64	PF	2019-12-14 13:56:00-03	134.74	83.90	2022-12-29 21:00:00.590251-03
551	551	7	64	PF	2018-03-13 08:48:00-03	30963.52	28648.37	2022-12-26 08:10:19-03
558	558	7	64	PF	2021-08-23 09:15:00-03	359.56	148.96	2022-12-29 20:59:59.756309-03
565	565	7	64	PF	2016-06-28 09:31:00-03	4723.79	4450.35	2022-12-29 20:59:59.361189-03
570	570	7	64	PF	2021-12-14 07:13:00-03	1634.64	1530.20	2022-12-29 20:59:07-03
595	595	7	64	PF	2016-10-11 12:15:00-03	244085.13	251588.81	2019-07-28 12:05:07-03
657	657	7	64	PF	2020-03-06 10:40:00-03	76149.84	75325.04	2022-12-29 20:59:59.664969-03
673	673	7	64	PF	2016-09-09 11:30:00-03	92837.59	86746.66	2022-08-19 14:46:19-03
686	686	7	64	PF	2021-05-26 07:49:00-03	9942.37	9726.47	2022-12-29 20:59:59.939569-03
702	702	7	64	PF	2017-05-25 12:10:00-03	26386.73	26892.41	2022-12-29 20:42:02-03
709	709	7	64	PF	2018-09-07 08:13:00-03	111450.85	107095.24	2022-12-28 01:19:32-03
722	722	7	64	PF	2018-05-16 10:22:00-03	13669.03	13065.00	2022-12-29 20:59:59.593951-03
738	738	7	64	PF	2018-12-19 14:10:00-02	17755.96	16268.02	2022-12-29 21:00:00.196002-03
746	746	7	64	PF	2021-08-27 12:36:00-03	6437.88	6676.28	2022-12-29 20:57:27-03
754	754	7	64	PF	2022-12-15 11:27:00-03	1251.16	810.28	2022-12-29 20:59:59.225241-03
757	757	7	64	PF	2016-12-10 13:46:00-02	7591.24	7573.15	2022-12-29 21:00:00.249453-03
792	792	7	64	PF	2018-12-08 10:54:00-02	40346.73	38036.24	2022-12-29 20:59:59.722139-03
818	818	7	64	PF	2021-05-08 09:31:00-03	40853.20	41083.46	2022-12-29 20:37:02-03
851	851	7	64	PF	2022-05-02 07:45:00-03	182.18	152.01	2022-12-29 20:59:59.256705-03
857	857	7	64	PF	2017-05-14 11:29:00-03	1985.66	1836.71	2022-12-29 20:35:39-03
865	865	7	64	PF	2019-05-06 07:03:00-03	11394.44	11738.35	2022-12-29 20:59:59.154938-03
873	873	7	64	PF	2021-05-25 12:15:00-03	179.77	171.20	2022-12-29 21:00:00.872384-03
874	874	7	64	PF	2021-11-20 07:46:00-03	187.61	67.71	2022-12-29 20:59:59.217707-03
917	917	7	64	PF	2016-05-18 08:07:00-03	2074.12	1891.00	2022-12-29 21:00:00.003824-03
928	928	7	64	PF	2022-12-16 07:46:00-03	1453.08	1108.93	2023-01-08 15:51:04.806542-03
936	936	7	64	PF	2017-03-27 13:43:00-03	95650.13	98589.38	2021-07-24 04:40:28-03
951	951	7	64	PF	2018-11-19 09:53:00-02	5283.16	1806.91	2021-05-05 16:10:09-03
957	957	7	64	PF	2020-08-10 12:07:00-03	14387.09	13140.68	2022-12-29 21:00:00.183784-03
973	973	7	64	PF	2020-12-25 12:51:00-03	3776.58	3691.88	2022-12-29 21:00:00.572579-03
992	992	7	64	PF	2020-02-13 09:01:00-03	2219.01	2138.16	2022-12-26 18:11:00-03
994	994	7	64	PF	2015-12-02 10:23:00-02	19719.41	19764.44	2022-12-29 20:59:36-03
7	7	7	84	PF	2018-09-12 10:08:00-03	98.65	23.43	2022-12-29 21:00:00.414866-03
98	98	7	84	PF	2018-02-19 08:29:00-03	110694.89	103238.17	2019-12-04 05:10:57-03
100	100	7	84	PF	2017-03-10 12:18:00-03	26815.90	25875.00	2022-12-29 21:00:00.976909-03
117	117	7	84	PF	2022-06-22 07:15:00-03	61939.96	61304.34	2022-12-29 20:59:59.413867-03
122	122	7	84	PF	2016-10-02 08:23:00-03	24.67	11.88	2019-02-18 10:05:43-03
128	128	7	84	PF	2018-05-22 09:30:00-03	8.60	264.73	2022-12-29 20:59:59.447373-03
133	133	7	84	PF	2022-12-22 07:05:00-03	264.91	163.95	2023-01-15 03:14:44.988615-03
137	137	7	84	PF	2017-01-06 12:34:00-02	67907.37	62273.86	2022-12-12 22:27:39-03
150	150	7	84	PF	2021-11-04 08:27:00-03	30578.58	29116.17	2022-12-29 20:59:59.875489-03
151	151	7	84	PF	2018-03-09 12:53:00-03	29082.30	27325.63	2022-07-16 04:45:02-03
159	159	7	84	PF	2022-06-24 10:01:00-03	435.90	410.81	2022-12-29 21:00:00.530312-03
174	174	7	84	PF	2017-04-21 12:02:00-03	23287.88	21444.25	2022-09-14 17:46:48-03
181	181	7	84	PF	2021-06-08 12:03:00-03	32738.63	30918.08	2022-12-29 20:51:04-03
204	204	7	84	PF	2021-02-23 13:46:00-03	7630.23	7643.50	2022-12-27 05:55:17-03
247	247	7	84	PF	2019-07-25 11:05:00-03	92174.68	85291.46	2022-12-29 20:59:59.872999-03
256	256	7	84	PF	2020-04-28 13:46:00-03	3.88	494.70	2022-11-02 14:44:43-03
262	262	7	84	PF	2019-10-19 08:14:00-03	78324.90	78285.28	2022-12-29 21:00:00.641084-03
272	272	7	84	PF	2021-08-25 07:48:00-03	185905.51	177022.98	2022-12-29 20:59:59.90242-03
281	281	7	84	PF	2016-06-27 12:06:00-03	1034.35	669.55	2022-12-29 21:00:00.171235-03
296	296	7	84	PF	2016-07-03 08:32:00-03	14857.75	12292.18	2022-07-30 11:40:55-03
311	311	7	84	PF	2021-10-13 09:56:00-03	2741.65	1066.09	2022-12-29 20:59:59.205706-03
318	318	7	84	PF	2017-03-06 09:50:00-03	49763.07	49705.32	2022-12-29 20:59:59.592804-03
330	330	7	84	PF	2022-06-11 12:44:00-03	25535.14	23786.80	2022-12-29 20:59:59.324736-03
334	334	7	84	PF	2016-10-08 08:12:00-03	20727.58	20722.60	2022-12-29 21:00:00.877463-03
349	349	7	84	PF	2019-01-07 13:47:00-02	28053.59	26634.40	2022-12-29 20:14:40-03
406	406	7	84	PF	2017-08-25 10:16:00-03	4672.45	4835.91	2022-12-29 20:59:10-03
439	439	7	84	PF	2019-05-18 08:42:00-03	1.22	4032.21	2022-12-29 20:59:59.300289-03
478	478	7	84	PF	2022-06-03 07:52:00-03	36880.00	38096.67	2022-12-29 21:00:00.309508-03
489	489	7	84	PF	2019-08-04 10:50:00-03	230.91	139.47	2022-12-29 20:59:59.583176-03
493	493	7	84	PF	2017-12-19 14:53:00-02	17359.52	15370.33	2022-12-29 21:00:00.916154-03
494	494	7	84	PF	2019-09-05 08:39:00-03	353.71	308.37	2022-12-29 20:57:44-03
508	508	7	84	PF	2019-05-18 11:07:00-03	14898.66	14102.08	2022-12-29 21:00:00.042409-03
519	519	7	84	PF	2021-09-14 08:54:00-03	797.46	187.19	2022-12-29 20:55:54-03
536	536	7	84	PF	2020-12-09 11:29:00-03	457.36	1236.60	2022-12-29 21:00:00.975642-03
561	561	7	84	PF	2019-01-20 10:19:00-02	9182.99	8913.97	2022-06-24 05:29:45-03
572	572	7	84	PF	2021-09-21 13:34:00-03	26.11	5.73	2022-12-29 20:59:59.148259-03
589	589	7	84	PF	2018-09-13 10:09:00-03	74008.72	71872.80	2020-06-04 18:51:06-03
593	593	7	84	PF	2019-01-12 08:56:00-02	56788.45	57631.09	2022-12-29 20:59:59.736375-03
607	607	7	84	PF	2015-12-04 09:48:00-02	1963.97	1855.39	2018-09-08 00:23:55-03
618	618	7	84	PF	2018-12-14 09:23:00-02	11413.91	10896.79	2020-05-13 09:18:10-03
627	627	7	84	PF	2020-12-14 10:51:00-03	143.34	11.61	2022-12-29 20:59:59.510087-03
645	645	7	84	PF	2022-05-24 10:33:00-03	296.44	270.09	2022-12-29 21:00:00.157528-03
658	658	7	84	PF	2017-02-07 10:10:00-02	10623.52	10255.33	2019-09-20 17:58:34-03
660	660	7	84	PF	2022-10-28 11:34:00-03	64380.39	61857.22	2022-12-29 21:00:00.202916-03
663	663	7	84	PF	2020-07-21 11:21:00-03	318.79	209.45	2022-12-29 20:59:59.30429-03
682	682	7	84	PF	2020-10-14 13:53:00-03	20.31	225.71	2022-12-29 21:00:00.673322-03
695	695	7	84	PF	2021-04-18 12:39:00-03	18491.94	17175.37	2022-12-29 21:00:00.577676-03
708	708	7	84	PF	2020-07-19 08:21:00-03	959.21	770.50	2022-11-04 01:20:04-03
729	729	7	84	PF	2020-08-21 07:13:00-03	267.79	223.21	2022-12-29 20:59:37-03
734	734	7	84	PF	2020-08-17 12:00:00-03	1021.10	953.19	2022-12-29 21:00:00.912619-03
745	745	7	84	PF	2017-04-23 12:36:00-03	24507.80	22446.31	2022-12-29 21:00:00.182844-03
803	803	7	84	PF	2021-11-17 09:01:00-03	71217.55	67168.87	2022-12-29 20:59:59.437175-03
822	822	7	84	PF	2022-12-16 12:58:00-03	238.97	212.94	2023-01-04 04:55:58.325386-03
827	827	7	84	PF	2022-10-24 11:44:00-03	107556.29	99308.13	2022-12-29 21:00:00.038647-03
860	860	7	84	PF	2021-04-15 11:16:00-03	41005.77	37774.11	2022-12-29 20:59:59.493746-03
882	882	7	84	PF	2020-09-20 09:10:00-03	12488.90	12478.81	2022-12-29 20:59:59.861598-03
884	884	7	84	PF	2017-08-06 08:01:00-03	13113.03	12932.07	2022-12-29 20:43:26-03
893	893	7	84	PF	2022-07-19 10:30:00-03	310.48	298.54	2022-12-29 20:59:59.983608-03
929	929	7	84	PF	2019-02-27 10:07:00-03	34456.31	32992.26	2022-11-13 02:06:34-03
942	942	7	84	PF	2015-10-24 13:54:00-02	48.26	292.65	2021-07-27 10:03:47-03
944	944	7	84	PF	2020-05-09 08:42:00-03	18246.87	16570.43	2022-12-29 21:00:00.292822-03
948	948	7	84	PF	2016-09-25 12:39:00-03	22981.42	23251.53	2022-12-29 21:00:00.750961-03
959	959	7	84	PF	2022-06-07 09:06:00-03	9579.72	10044.11	2022-12-29 20:59:59.868973-03
964	964	7	84	PF	2021-06-24 11:15:00-03	938.70	17.60	2022-12-29 20:59:59.423736-03
969	969	7	84	PF	2020-10-18 07:41:00-03	19705.84	19084.52	2022-12-29 21:00:00.563535-03
986	986	7	84	PF	2016-01-23 10:45:00-02	3674.61	3468.33	2019-01-19 03:34:40-02
999	999	7	84	PF	2017-08-25 12:13:00-03	18697.46	18628.42	2022-12-29 20:59:59.752141-03
12	12	7	85	PF	2016-01-13 10:28:00-02	437.29	405.41	2022-12-29 20:59:58-03
13	13	7	85	PF	2022-12-16 08:20:00-03	229.78	212.50	2023-01-08 04:02:40.717016-03
20	20	7	85	PF	2021-05-20 12:06:00-03	50155.81	49629.64	2022-12-29 20:59:59.134153-03
58	58	7	85	PF	2017-11-02 10:54:00-02	55549.86	57199.63	2022-12-29 21:00:00.41749-03
63	63	7	85	PF	2019-04-10 09:23:00-03	2354.25	2160.99	2022-12-29 21:00:00.08759-03
91	91	7	85	PF	2020-02-17 07:45:00-03	51889.38	50612.74	2022-12-29 20:59:30-03
123	123	7	85	PF	2021-10-16 12:35:00-03	308.13	186.79	2022-12-29 20:59:59.004416-03
182	182	7	85	PF	2020-11-04 09:06:00-03	6257.19	6395.69	2022-12-29 20:59:59.937321-03
273	273	7	85	PF	2020-04-27 08:42:00-03	65.49	29.70	2022-12-29 21:00:00.680274-03
308	308	7	85	PF	2018-06-26 11:23:00-03	28868.04	26440.84	2022-12-29 21:00:00.775337-03
314	314	7	85	PF	2017-02-11 10:07:00-02	19172.35	17788.25	2022-12-23 05:14:19-03
324	324	7	85	PF	2022-01-11 12:19:00-03	123.14	103.13	2022-12-29 21:00:00.277362-03
335	335	7	85	PF	2022-01-02 08:09:00-03	751.07	709.01	2022-12-29 20:59:59.05189-03
337	337	7	85	PF	2017-08-03 11:16:00-03	36506.97	37852.09	2022-12-29 05:42:23-03
338	338	7	85	PF	2016-03-07 11:26:00-03	185276.12	179836.59	2022-12-29 21:00:00.75186-03
345	345	7	85	PF	2017-12-08 11:24:00-02	11346.32	10598.12	2022-12-29 21:00:00.302612-03
348	348	7	85	PF	2018-03-03 08:58:00-03	51345.40	48405.15	2021-10-06 01:37:41-03
367	367	7	85	PF	2015-08-21 08:07:00-03	15067.75	15142.78	2021-11-06 09:28:06-03
369	369	7	85	PF	2022-10-09 08:31:00-03	2372.80	2392.14	2022-12-29 21:00:00.630579-03
378	378	7	85	PF	2018-03-18 09:36:00-03	1483.80	1454.99	2020-12-28 04:37:42-03
384	384	7	85	PF	2022-03-10 10:20:00-03	26811.23	25982.42	2022-12-29 20:59:59.194529-03
391	391	7	85	PF	2020-06-21 08:39:00-03	221113.68	215962.42	2022-10-16 04:03:56-03
392	392	7	85	PF	2015-12-26 13:09:00-02	4970.32	4563.98	2019-09-22 21:15:52-03
396	396	7	85	PF	2021-09-24 08:11:00-03	7540.11	7091.65	2022-12-29 21:00:00.66657-03
397	397	7	85	PF	2018-10-13 07:51:00-03	40026.37	36169.34	2022-12-29 19:02:26-03
409	409	7	85	PF	2015-11-08 12:10:00-02	17764.62	18283.47	2021-04-07 11:37:17-03
458	458	7	85	PF	2017-07-02 11:47:00-03	55600.62	57884.81	2022-12-29 20:59:59.14525-03
463	463	7	85	PF	2017-04-02 10:32:00-03	13633.90	13891.76	2020-11-08 12:13:48-03
465	465	7	85	PF	2017-10-07 11:54:00-03	6002.17	6030.19	2021-08-25 13:37:01-03
469	469	7	85	PF	2018-09-27 08:50:00-03	43152.17	45107.74	2022-12-29 20:59:59.897207-03
471	471	7	85	PF	2022-10-20 13:02:00-03	592.73	529.53	2022-12-29 20:59:59.822754-03
483	483	7	85	PF	2022-10-23 08:12:00-03	59749.51	56176.71	2022-12-29 21:00:00.904481-03
502	502	7	85	PF	2021-12-19 12:05:00-03	58387.64	58427.81	2022-12-29 21:00:00.447584-03
503	503	7	85	PF	2020-11-25 10:30:00-03	5085.35	5111.28	2022-12-29 20:59:59.909101-03
511	511	7	85	PF	2018-11-07 09:57:00-02	36503.11	38393.28	2022-12-18 14:28:23-03
522	522	7	85	PF	2016-11-05 11:26:00-02	1004.89	377.69	2021-01-25 16:40:47-03
524	524	7	85	PF	2019-11-02 08:32:00-03	75259.85	70788.36	2022-12-29 20:53:37-03
609	609	7	85	PF	2017-05-14 12:03:00-03	57623.75	58782.09	2021-09-04 03:56:17-03
619	619	7	85	PF	2020-04-02 11:43:00-03	11.72	789.16	2022-05-21 00:34:21-03
675	675	7	85	PF	2019-04-24 10:55:00-03	43784.88	40696.79	2022-12-29 21:00:00.544625-03
694	694	7	85	PF	2022-04-07 11:35:00-03	137.44	98.67	2022-12-29 20:59:59.153504-03
696	696	7	85	PF	2021-12-24 10:02:00-03	331.32	133.89	2022-12-29 20:59:59.365934-03
719	719	7	85	PF	2016-08-16 10:16:00-03	13048.59	13317.15	2019-12-20 16:18:15-03
724	724	7	85	PF	2019-08-07 10:31:00-03	1378.00	1349.54	2022-09-14 02:40:11-03
731	731	7	85	PF	2017-07-21 08:57:00-03	857.75	512.93	2022-12-26 17:38:55-03
772	772	7	85	PF	2017-06-08 08:13:00-03	53692.84	50042.79	2021-11-12 10:19:09-03
777	777	7	85	PF	2022-01-07 10:03:00-03	1200.76	577.59	2022-12-29 20:59:59.536609-03
791	791	7	85	PF	2015-10-21 11:49:00-02	5609.51	4787.19	2022-12-27 20:54:19-03
795	795	7	85	PF	2022-02-25 12:08:00-03	100.64	51.65	2022-12-29 20:59:59.852052-03
806	806	7	85	PF	2018-05-02 10:32:00-03	584.98	576.96	2022-12-29 20:48:23-03
811	811	7	85	PF	2018-08-06 08:04:00-03	7630.79	7722.45	2022-08-24 00:53:49-03
812	812	7	85	PF	2018-10-24 09:26:00-03	15943.42	16354.83	2022-06-29 12:52:37-03
823	823	7	85	PF	2022-01-18 09:45:00-03	383.59	357.27	2022-12-29 20:59:59.916742-03
847	847	7	85	PF	2017-10-23 10:25:00-02	84.57	35.30	2022-08-17 22:29:44-03
908	908	7	85	PF	2021-04-02 07:22:00-03	121.07	146.49	2022-12-29 20:59:59.051283-03
933	933	7	85	PF	2015-08-12 10:49:00-03	18663.66	17579.49	2017-10-10 12:46:46-03
965	965	7	85	PF	2016-08-28 11:28:00-03	321.13	411.64	2022-12-29 21:00:00.792144-03
976	976	7	85	PF	2015-09-16 12:49:00-03	68294.77	64946.46	2019-08-14 17:05:43-03
32	32	7	86	PF	2017-12-14 12:59:00-02	17618.68	16884.65	2020-10-04 04:29:28-03
88	88	7	86	PF	2020-10-07 11:15:00-03	13428.22	13974.82	2022-12-29 20:59:59.422843-03
99	99	7	86	PF	2017-10-14 09:58:00-03	6520.00	6492.20	2022-12-29 21:00:00.121381-03
101	101	7	86	PF	2018-11-05 12:17:00-02	111513.04	105076.54	2022-12-29 20:44:00-03
121	121	7	86	PF	2021-02-08 08:22:00-03	560.09	167.85	2022-12-29 20:59:59.126199-03
156	156	7	86	PF	2018-09-03 12:21:00-03	77724.42	77618.77	2021-08-29 11:31:08-03
184	184	7	86	PF	2018-11-12 11:09:00-02	30326.43	31954.14	2022-12-29 21:00:00.746947-03
194	194	7	86	PF	2016-12-18 12:20:00-02	6724.23	6042.25	2021-12-11 01:38:39-03
214	214	7	86	PF	2017-04-04 11:29:00-03	62.30	51.56	2022-12-29 20:59:59.589471-03
219	219	7	86	PF	2022-02-17 07:52:00-03	45527.32	46013.39	2022-12-29 21:00:00.69418-03
236	236	7	86	PF	2020-02-25 08:42:00-03	21089.26	21928.04	2022-12-29 20:59:59.882864-03
268	268	7	86	PF	2016-07-06 13:25:00-03	745.35	548.68	2022-04-21 00:27:50-03
282	282	7	86	PF	2017-10-03 09:41:00-03	156.66	78.30	2022-12-29 21:00:00.158915-03
287	287	7	86	PF	2020-07-20 10:04:00-03	37008.84	38511.38	2022-12-29 20:59:59.21128-03
305	305	7	86	PF	2021-07-13 10:43:00-03	275.74	187.48	2022-12-29 21:00:00.012666-03
336	336	7	86	PF	2020-10-18 10:03:00-03	5.06	33.56	2022-12-29 21:00:00.340335-03
344	344	7	86	PF	2022-05-25 10:35:00-03	142.34	26.17	2022-12-29 20:59:59.41915-03
400	400	7	86	PF	2018-04-21 12:16:00-03	27.32	9013.16	2022-12-29 20:59:59.930257-03
401	401	7	86	PF	2020-12-18 08:53:00-03	51461.90	51296.90	2022-08-06 23:42:20-03
403	403	7	86	PF	2020-03-17 10:11:00-03	53898.49	50265.66	2022-12-29 20:59:59.751958-03
404	404	7	86	PF	2022-06-05 12:57:00-03	32344.56	31038.09	2022-12-29 21:00:00.432683-03
426	426	7	86	PF	2022-06-27 08:26:00-03	152392.83	139689.94	2022-12-29 21:00:00.903816-03
436	436	7	86	PF	2019-12-10 09:32:00-03	8634.92	9025.44	2022-12-29 20:59:59.671918-03
445	445	7	86	PF	2019-10-18 11:44:00-03	9499.42	7373.57	2022-12-29 15:13:01-03
470	470	7	86	PF	2016-10-03 11:01:00-03	14772.14	12201.82	2022-12-29 20:59:59.527107-03
474	474	7	86	PF	2020-01-12 07:03:00-03	116.68	13.08	2022-12-29 20:59:59.796608-03
563	563	7	86	PF	2021-07-10 08:51:00-03	46720.60	46232.24	2022-12-29 21:00:00.239283-03
569	569	7	86	PF	2016-11-25 10:47:00-02	38.12	10.87	2022-12-29 20:59:59.599916-03
583	583	7	86	PF	2020-06-25 11:02:00-03	380.63	128.17	2022-12-29 20:59:59.389092-03
594	594	7	86	PF	2019-11-26 11:01:00-03	1648.62	1131.10	2022-11-26 21:35:22-03
605	605	7	86	PF	2022-05-02 13:57:00-03	50226.31	49704.08	2022-12-29 21:00:00.234545-03
629	629	7	86	PF	2018-06-15 10:15:00-03	4654.49	4310.24	2022-12-29 20:49:27-03
654	654	7	86	PF	2019-05-24 09:34:00-03	48933.50	46182.71	2022-12-29 20:59:59.98354-03
665	665	7	86	PF	2019-12-11 11:35:00-03	15668.52	16088.41	2022-12-29 20:59:59.131184-03
669	669	7	86	PF	2016-04-15 11:26:00-03	67205.88	55598.12	2020-04-13 22:45:56-03
683	683	7	86	PF	2022-03-06 10:25:00-03	4130.39	4337.77	2022-12-29 21:00:00.511657-03
707	707	7	86	PF	2016-10-22 12:14:00-02	60833.23	59617.25	2019-01-13 11:11:29-02
748	748	7	86	PF	2019-06-19 08:57:00-03	49285.01	50788.55	2022-12-29 21:00:00.011986-03
784	784	7	86	PF	2020-07-27 09:34:00-03	373.39	331.31	2022-12-29 20:59:59.156407-03
788	788	7	86	PF	2021-06-11 08:11:00-03	38.44	243.41	2022-12-29 21:00:00.956241-03
809	809	7	86	PF	2021-05-18 13:15:00-03	901.68	568.80	2022-12-29 20:59:59.380966-03
816	816	7	86	PF	2017-04-18 12:01:00-03	157.97	57.04	2022-12-23 10:45:19-03
835	835	7	86	PF	2019-12-09 08:43:00-03	39400.28	38090.60	2022-12-29 20:59:59.787445-03
843	843	7	86	PF	2019-08-09 07:35:00-03	11976.37	11226.09	2021-07-01 23:15:55-03
854	854	7	86	PF	2017-04-13 09:22:00-03	656.25	652.94	2022-12-29 20:59:59.220665-03
859	859	7	86	PF	2020-11-03 08:41:00-03	4935.65	1943.39	2022-12-29 21:00:00.920671-03
867	867	7	86	PF	2016-10-14 09:02:00-03	112726.72	112686.21	2019-03-15 08:54:30-03
885	885	7	86	PF	2018-03-01 12:45:00-03	16065.41	15859.56	2020-09-01 23:39:39-03
894	894	7	86	PF	2019-06-04 09:01:00-03	39102.76	36479.20	2022-10-23 16:09:11-03
901	901	7	86	PF	2020-01-18 10:48:00-03	18.92	310.45	2022-12-29 20:59:59.192828-03
907	907	7	86	PF	2022-08-01 09:44:00-03	4204.58	4444.39	2022-12-29 21:00:00.499121-03
921	921	7	86	PF	2021-09-03 12:52:00-03	334.41	297.92	2022-12-29 20:59:59.728015-03
922	922	7	86	PF	2022-08-17 08:05:00-03	38032.11	36903.26	2022-12-29 21:00:00.103515-03
931	931	7	86	PF	2020-09-14 11:09:00-03	16674.27	16089.49	2022-12-29 21:00:00.401417-03
949	949	7	86	PF	2016-07-07 07:30:00-03	762.50	635.62	2021-12-26 05:41:06-03
961	961	7	86	PF	2020-05-13 08:41:00-03	91464.29	91423.61	2022-12-29 20:59:59.488865-03
962	962	7	86	PF	2022-07-19 12:11:00-03	15163.99	15247.75	2022-12-29 20:59:59.609314-03
978	978	7	86	PF	2022-03-07 11:38:00-03	15455.42	15291.84	2022-12-29 21:00:00.122916-03
985	985	7	86	PF	2018-01-18 13:15:00-02	24814.11	22495.77	2021-06-25 21:54:13-03
30	30	8	33	PF	2018-01-26 12:53:00-02	129.66	957.15	2022-12-29 21:00:00.024953-03
475	475	8	33	PF	2020-05-04 10:45:00-03	82402.71	82534.20	2022-12-29 20:59:59.304362-03
858	858	8	33	PF	2018-11-28 09:24:00-02	628.78	297.11	2022-04-14 13:47:44-03
38	38	8	35	PF	2021-08-23 09:15:00-03	31493.59	31765.42	2022-12-29 21:00:00.550629-03
125	125	8	35	PF	2019-04-08 09:41:00-03	10863.58	12932.35	2022-12-29 20:59:57-03
428	428	8	35	PF	2019-11-27 11:29:00-03	61602.49	56521.82	2022-12-29 20:59:59.96624-03
526	526	8	35	PF	2018-09-16 12:21:00-03	37666.72	37588.75	2022-12-29 17:03:55-03
869	869	8	35	PF	2021-04-09 10:01:00-03	1807.35	1835.42	2022-12-29 20:59:59.261903-03
872	872	8	35	PF	2021-01-16 07:38:00-03	74.70	55.00	2022-12-29 21:00:00.875846-03
120	120	8	40	PF	2021-11-18 07:05:00-03	448.52	400.95	2022-12-29 20:59:59.02948-03
453	453	8	40	PF	2022-01-14 10:16:00-03	20.26	218.06	2022-12-29 20:59:59.573525-03
918	918	8	40	PF	2020-05-16 08:58:00-03	666.10	502.46	2022-12-29 19:20:07-03
209	209	8	51	PF	2021-02-07 09:38:00-03	21.63	8.36	2022-12-29 20:59:59.344628-03
407	407	8	51	PF	2019-05-18 10:18:00-03	6002.91	5776.05	2021-11-15 07:22:45-03
556	556	8	51	PF	2020-06-08 11:51:00-03	165.02	1003.86	2022-12-29 20:59:57-03
560	560	8	51	PF	2020-07-14 10:09:00-03	40.80	17.21	2022-12-11 19:19:08-03
950	950	8	51	PF	2020-10-23 11:33:00-03	184.49	67.75	2022-07-04 21:56:30-03
44	44	8	68	PF	2020-09-07 11:32:00-03	26049.30	24401.26	2022-11-05 18:31:22-03
639	639	8	68	PF	2020-04-16 10:45:00-03	119106.69	122899.57	2022-11-18 09:13:00-03
736	736	8	68	PF	2021-11-06 12:31:00-03	6682.74	6102.12	2022-12-29 20:59:59.081001-03
825	825	8	68	PF	2022-04-28 11:01:00-03	8502.88	7492.98	2022-12-29 21:00:00.772394-03
69	69	8	80	PF	2019-10-26 11:52:00-03	152.50	103.48	2022-12-29 20:59:59.509262-03
288	288	8	80	PF	2018-10-08 07:40:00-03	527.60	437.70	2022-12-29 21:00:00.730093-03
359	359	8	80	PF	2019-01-15 09:27:00-02	60095.91	57983.04	2022-05-10 17:02:31-03
644	644	8	80	PF	2022-11-06 09:02:00-03	651.23	610.89	2022-12-29 21:00:00.212304-03
693	693	8	80	PF	2020-01-21 10:12:00-03	162.09	75.28	2022-12-29 20:59:59.334376-03
743	743	8	80	PF	2019-05-06 08:39:00-03	14112.22	12225.51	2022-12-29 20:40:09-03
50	50	8	94	PF	2021-11-17 13:16:00-03	292.62	92.12	2022-12-29 21:00:00.263525-03
672	672	8	94	PF	2019-07-14 09:16:00-03	15905.06	14492.84	2022-12-29 20:59:55-03
853	853	8	94	PF	2020-11-21 08:32:00-03	232.55	223.73	2022-12-29 21:00:00.86864-03
253	253	9	1	PF	2022-05-27 08:17:00-03	324.39	232.78	2022-12-29 21:00:00.299986-03
313	313	9	1	PF	2022-08-22 10:17:00-03	4056.66	4254.34	2022-12-29 20:59:59.033326-03
591	591	9	1	PF	2022-05-19 08:46:00-03	20638.03	19948.77	2022-12-29 21:00:00.46683-03
793	793	9	1	PF	2020-05-03 12:46:00-03	19031.04	19194.43	2022-12-29 21:00:00.087299-03
815	815	9	1	PF	2021-06-21 11:04:00-03	174.40	124.26	2022-12-29 20:59:59.904627-03
111	111	9	11	PF	2021-11-02 08:34:00-03	8803.35	8911.52	2022-12-29 21:00:00.70592-03
652	652	9	11	PF	2020-10-23 12:23:00-03	88153.38	80069.18	2022-08-29 13:01:05-03
726	726	9	11	PF	2019-11-06 12:14:00-03	6821.06	7092.00	2022-12-29 21:00:00.545392-03
25	25	9	25	PF	2022-09-04 07:40:00-03	9489.89	9647.99	2022-12-29 20:59:59.761029-03
132	132	9	25	PF	2022-08-18 08:53:00-03	57047.00	57686.22	2022-12-29 21:00:00.794804-03
585	585	9	25	PF	2022-10-20 08:04:00-03	44649.39	40913.97	2022-12-29 21:00:00.696077-03
96	96	9	38	PF	2022-08-15 10:30:00-03	317.95	257.75	2022-12-29 20:59:59.221345-03
443	443	9	38	PF	2022-01-03 08:42:00-03	470.73	383.61	2022-12-29 20:59:59.742301-03
713	713	9	38	PF	2019-10-10 07:50:00-03	6769.58	6828.53	2022-12-29 20:59:59.698062-03
66	66	9	39	PF	2021-05-19 10:18:00-03	134.87	78.44	2022-06-23 23:32:39-03
321	321	9	39	PF	2020-04-24 07:58:00-03	706.22	444.63	2022-12-29 20:59:59.798756-03
916	916	9	39	PF	2020-01-17 08:30:00-03	20401.10	18537.45	2022-12-29 21:00:00.050997-03
947	947	9	39	PF	2022-09-02 12:11:00-03	36730.38	37095.21	2022-12-29 21:00:00.193547-03
10	10	9	75	PF	2020-11-03 11:04:00-03	30742.04	28682.48	2022-12-29 21:00:00.125945-03
34	34	9	75	PF	2022-08-02 12:03:00-03	31583.46	29187.79	2022-12-29 21:00:00.58848-03
711	711	9	75	PF	2022-12-08 11:47:00-03	342.77	235.76	2023-01-05 01:57:57.508429-03
278	278	9	90	PF	2020-04-17 12:47:00-03	47525.72	47220.72	2022-12-29 20:59:59.45867-03
512	512	9	90	PF	2020-05-12 09:40:00-03	5687.07	4791.86	2022-02-06 11:04:31-03
648	648	9	90	PF	2021-07-28 12:32:00-03	5011.14	4585.31	2022-12-29 21:00:00.611082-03
904	904	9	90	PF	2020-12-16 11:36:00-03	153177.78	143607.04	2022-12-29 05:00:16-03
41	41	9	91	PF	2020-02-28 09:44:00-03	53.16	91.53	2022-12-29 21:00:00.427258-03
79	79	9	91	PF	2022-12-03 07:51:00-03	883.05	642.80	2022-12-29 21:00:00.072279-03
136	136	9	91	PF	2021-03-24 10:43:00-03	34431.60	32145.32	2022-12-29 20:59:59.053442-03
257	257	9	91	PF	2021-05-20 11:11:00-03	2937.33	2641.36	2022-12-29 21:00:00.442241-03
770	770	9	91	PF	2022-04-04 09:34:00-03	62511.09	29401.58	2022-12-29 20:59:59.127275-03
466	466	10	8	PF	2022-10-26 11:13:00-03	7847.42	9351.63	2022-12-29 21:00:00.319185-03
764	764	10	8	PF	2022-09-08 08:06:00-03	65495.19	64823.14	2022-12-29 20:59:59.964121-03
488	488	10	18	PF	2022-07-16 07:16:00-03	8663.61	6836.57	2022-12-29 21:00:00.966349-03
395	395	10	95	PF	2022-12-01 12:27:00-03	23156.21	21578.33	2022-12-29 21:00:00.607818-03
355	355	10	100	PF	2022-10-22 11:50:00-03	10680.15	10336.34	2022-12-29 20:59:59.739032-03
\.


--
-- Data for Name: propostas_credito; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.propostas_credito (cod_proposta, cod_cliente, cod_colaborador, data_entrada_proposta, taxa_juros_mensal, valor_proposta, valor_financiamento, valor_entrada, valor_prestacao, quantidade_parcelas, carencia, status_proposta) FROM stdin;
116	338	1	2014-05-30 15:43:12-03	0.0194	36199.95	50032.03	13832.08	1045.22	58	0	Enviada
715	45	1	2021-06-17 10:09:32-03	0.0131	12897.54	19848.55	6951.01	232.12	100	1	Enviada
755	494	1	2021-08-10 01:24:02-03	0.0203	122236.31	187154.36	64918.05	3673.52	56	0	Enviada
953	381	1	2011-02-23 04:57:58-03	0.0167	47784.65	59530.61	11745.96	955.04	109	3	Enviada
1046	176	1	2010-11-15 14:03:28-02	0.0211	178393.04	214294.19	35901.15	10219.66	22	4	Enviada
1171	732	1	2020-05-04 18:51:58-03	0.0242	40476.96	56464.88	15987.92	20976.06	2	0	Enviada
1434	560	1	2020-01-30 21:02:49-03	0.0169	137637.62	192947.94	55310.32	2861.61	100	0	Enviada
1744	159	1	2016-03-04 17:31:58-03	0.0180	1317.13	1605.63	288.50	36.08	60	2	Enviada
1864	776	1	2019-09-13 07:21:31-03	0.0110	106127.47	128472.96	22345.49	1774.93	98	4	Enviada
1928	421	1	2014-05-08 12:43:27-03	0.0200	3455.61	6668.98	3213.37	211.33	20	2	Enviada
41	608	2	2021-03-04 05:08:46-03	0.0136	168359.68	235794.55	67434.87	35058.12	5	4	Enviada
416	342	2	2015-10-11 11:28:01-03	0.0187	112227.05	215028.75	102801.70	3156.68	59	1	Enviada
581	81	2	2022-03-19 20:43:49-03	0.0183	59901.04	112333.57	52432.53	1309.80	100	6	Enviada
678	788	2	2011-02-03 04:12:10-02	0.0165	111091.75	167331.74	56239.99	5459.06	25	3	Enviada
1272	673	2	2012-10-13 04:02:05-03	0.0204	53294.79	69167.42	15872.63	1370.96	78	6	Enviada
1469	895	2	2022-04-08 03:23:32-03	0.0199	38892.28	68709.07	29816.79	1298.52	46	6	Enviada
91	117	3	2010-03-23 22:05:13-03	0.0174	71838.24	111779.56	39941.32	1616.71	86	1	Enviada
102	859	3	2021-06-16 06:12:09-03	0.0164	107696.23	199719.09	92022.86	2441.64	79	2	Enviada
523	587	3	2012-12-31 05:51:31-02	0.0190	42714.08	51641.15	8927.07	1835.94	31	6	Enviada
979	778	3	2014-06-28 13:57:58-03	0.0238	104763.13	157840.73	53077.60	2717.98	106	4	Enviada
1564	419	3	2013-11-09 22:28:30-02	0.0218	13502.18	21466.99	7964.81	1668.47	9	5	Enviada
86	938	4	2013-06-17 20:23:46-03	0.0135	3184.44	5522.97	2338.53	61.34	90	5	Enviada
209	635	4	2011-06-10 19:53:56-03	0.0142	161192.35	233647.20	72454.85	19205.79	9	6	Enviada
226	88	4	2010-05-08 03:27:52-03	0.0194	121138.65	154297.83	33159.18	2680.15	109	6	Enviada
543	710	4	2014-06-19 15:52:17-03	0.0199	41698.07	66347.38	24649.31	1081.40	74	4	Enviada
886	138	4	2016-06-26 20:19:30-03	0.0201	17467.40	25261.08	7793.68	522.54	56	1	Enviada
1045	934	4	2016-05-15 18:08:53-03	0.0212	180566.24	239033.99	58467.75	13425.47	16	3	Enviada
1566	648	4	2015-10-07 11:06:02-03	0.0165	15578.91	30167.01	14588.10	4056.70	4	4	Enviada
1573	395	4	2013-01-20 09:07:21-02	0.0125	131608.20	167117.91	35509.71	2490.08	87	3	Enviada
23	358	5	2020-05-30 01:42:27-03	0.0113	13338.29	18727.61	5389.32	252.24	81	0	Enviada
352	249	5	2014-12-05 23:59:40-02	0.0093	27214.51	33278.95	6064.44	3166.18	9	3	Enviada
1291	928	5	2013-12-30 16:42:28-02	0.0240	101775.63	147301.56	45525.93	9206.44	13	4	Enviada
308	335	6	2022-08-09 12:51:27-03	0.0204	138074.89	177330.72	39255.83	3698.41	71	3	Enviada
1120	114	6	2011-10-04 05:23:32-03	0.0129	6214.07	9537.71	3323.64	282.84	26	4	Enviada
1390	806	6	2018-11-15 00:42:49-02	0.0243	103211.59	124934.78	21723.19	5401.37	26	2	Enviada
1639	827	6	2013-12-09 20:00:45-02	0.0129	155881.86	206120.59	50238.73	3091.64	82	2	Enviada
72	983	7	2022-03-21 01:49:19-03	0.0108	60091.11	109302.14	49211.03	2755.60	25	1	Enviada
1121	572	8	2010-01-07 10:30:10-02	0.0247	185357.39	234345.58	48988.19	4913.90	110	2	Enviada
1669	65	8	2022-09-23 22:37:57-03	0.0179	88993.97	116726.64	27732.67	1851.35	111	5	Enviada
906	787	10	2015-09-17 04:18:09-03	0.0157	106954.15	183309.59	76355.44	2471.99	73	4	Enviada
1861	905	10	2012-02-20 10:54:02-02	0.0210	2130.44	2850.11	719.67	70.04	49	3	Enviada
1911	127	10	2021-02-17 13:43:26-03	0.0159	55943.24	72010.93	16067.69	1425.58	62	5	Enviada
1956	336	10	2015-11-20 17:09:20-02	0.0165	62196.85	81395.72	19198.87	2031.15	43	2	Enviada
487	635	11	2015-05-05 15:40:19-03	0.0182	13134.53	20036.49	6901.96	355.12	62	4	Enviada
553	511	11	2014-04-07 06:13:03-03	0.0084	54924.84	67689.85	12765.01	1498.33	44	3	Enviada
230	561	12	2014-06-16 22:23:40-03	0.0117	177882.89	225362.93	47480.04	20938.93	9	3	Enviada
478	551	12	2016-01-06 06:08:03-02	0.0223	80871.28	113384.93	32513.65	2038.15	98	3	Enviada
1222	876	12	2018-07-18 06:59:32-03	0.0095	56134.72	77171.78	21037.06	2159.26	30	6	Enviada
1413	498	12	2020-08-21 09:01:47-03	0.0177	1702.59	2118.23	415.64	97.78	21	1	Enviada
1728	545	12	2017-12-17 10:46:31-02	0.0190	31701.49	49986.53	18285.04	675.64	118	1	Enviada
1875	973	12	2017-05-28 03:08:20-03	0.0127	113668.20	145713.86	32045.66	5335.22	25	2	Enviada
53	489	13	2021-11-29 22:11:53-03	0.0123	68601.86	90359.89	21758.03	1305.72	85	5	Enviada
576	905	13	2018-07-29 03:28:27-03	0.0136	74902.22	108072.51	33170.29	1599.37	75	0	Enviada
673	927	13	2020-07-20 06:16:36-03	0.0213	122043.34	193809.71	71766.37	4187.84	46	4	Enviada
875	429	13	2019-02-04 15:53:39-02	0.0169	142939.10	185095.87	42156.77	73286.36	2	3	Enviada
1159	332	15	2017-08-14 08:30:11-03	0.0123	12489.68	19344.59	6854.91	326.56	52	4	Enviada
1339	429	15	2015-11-10 11:13:00-02	0.0194	36926.13	44548.77	7622.64	996.83	66	0	Enviada
1846	642	15	2018-02-13 21:35:58-02	0.0119	117394.81	227027.50	109632.69	7668.48	17	1	Enviada
22	355	16	2021-05-14 15:13:36-03	0.0087	31123.47	42496.51	11373.04	496.49	91	6	Enviada
46	575	16	2019-03-14 01:06:15-03	0.0209	43183.00	78427.34	35244.34	986.70	119	6	Enviada
839	174	16	2019-10-05 12:24:50-03	0.0223	145336.82	195421.72	50084.90	16375.16	10	2	Enviada
896	705	16	2012-04-24 08:31:09-03	0.0101	114732.00	216856.25	102124.25	1838.67	99	5	Enviada
1251	533	16	2017-04-22 21:37:41-03	0.0195	26362.31	33955.36	7593.05	1673.69	19	4	Enviada
1393	853	16	2012-06-05 17:29:13-03	0.0131	30487.99	52975.78	22487.79	823.34	51	0	Enviada
1540	984	16	2022-03-16 21:37:06-03	0.0237	134454.66	170958.40	36503.74	3953.81	70	3	Enviada
1799	688	16	2015-01-14 03:30:39-02	0.0101	152770.39	208553.81	55783.42	2273.23	113	2	Enviada
483	28	17	2015-04-03 01:34:11-03	0.0106	13056.94	18009.22	4952.28	285.16	63	3	Enviada
508	857	17	2017-08-21 15:47:21-03	0.0082	21295.36	40141.99	18846.63	286.72	115	1	Enviada
687	2	17	2014-06-18 09:27:41-03	0.0184	59582.47	89576.90	29994.43	1263.25	111	3	Enviada
1602	858	17	2017-08-01 06:32:19-03	0.0225	12469.46	20343.34	7873.88	322.16	92	6	Enviada
1815	192	17	2019-07-24 23:13:59-03	0.0216	57306.66	77305.08	19998.42	58544.49	1	0	Enviada
104	921	18	2012-04-02 05:36:34-03	0.0190	19286.33	35071.35	15785.02	419.33	110	4	Enviada
231	189	18	2016-12-10 14:16:53-02	0.0218	4663.69	8762.78	4099.09	113.18	106	2	Enviada
302	448	18	2013-02-16 21:46:50-02	0.0133	206514.64	246916.06	40401.42	8172.77	31	6	Enviada
306	813	18	2011-03-31 19:02:19-03	0.0195	61031.81	102755.99	41724.18	6776.70	10	5	Enviada
353	750	18	2012-04-15 03:14:28-03	0.0153	108348.99	197148.24	88799.25	3856.77	37	4	Enviada
359	903	18	2012-05-06 05:07:37-03	0.0165	112299.60	167978.57	55678.97	2657.71	73	5	Enviada
505	853	18	2011-02-18 09:55:27-02	0.0087	133575.20	228888.78	95313.58	2247.99	84	2	Enviada
1466	105	18	2015-08-16 01:19:44-03	0.0182	101145.62	194336.22	93190.60	2263.89	93	1	Enviada
1914	523	18	2017-07-15 02:48:38-03	0.0084	173873.84	225277.05	51403.21	3104.54	76	2	Enviada
1920	516	18	2020-02-21 18:40:43-03	0.0203	53781.13	63393.69	9612.56	1475.66	67	6	Enviada
88	194	19	2012-10-03 05:20:31-03	0.0105	70119.78	134983.88	64864.10	3738.11	21	6	Enviada
413	715	19	2017-04-11 18:53:37-03	0.0119	18053.35	33305.88	15252.53	1120.15	18	0	Enviada
535	367	19	2017-10-17 13:19:55-02	0.0175	143356.77	183308.52	39951.75	3236.78	86	1	Enviada
552	804	19	2018-10-13 19:25:25-03	0.0139	104362.53	132399.37	28036.84	1956.38	98	4	Enviada
685	845	19	2011-05-15 18:38:19-03	0.0103	49646.84	91935.02	42288.18	1063.15	64	3	Enviada
1126	213	19	2021-03-04 06:06:04-03	0.0095	87235.55	103873.90	16638.35	44240.31	2	5	Enviada
1113	70	20	2021-01-06 02:30:01-03	0.0201	71453.78	104460.86	33007.08	1663.61	100	4	Enviada
180	641	21	2013-09-11 20:11:48-03	0.0192	162205.41	233950.18	71744.77	3769.63	92	5	Enviada
1515	845	21	2018-09-24 06:20:31-03	0.0143	51909.58	67405.30	15495.72	1816.47	37	2	Enviada
289	175	22	2013-12-17 21:03:46-02	0.0195	37514.33	67230.70	29716.37	811.48	120	4	Enviada
467	522	22	2010-04-07 04:20:32-03	0.0178	150255.60	222421.41	72165.81	9829.55	18	6	Enviada
1235	359	22	2021-01-08 18:11:43-03	0.0116	187941.97	225117.54	37175.57	190122.09	1	6	Enviada
1555	562	22	2017-05-05 08:31:55-03	0.0102	125720.20	211913.34	86193.14	2186.74	87	6	Enviada
1726	56	22	2014-05-24 19:58:13-03	0.0149	57910.76	77127.49	19216.73	3691.73	18	4	Enviada
375	426	23	2021-05-19 22:38:29-03	0.0093	3655.90	4480.41	824.51	61.02	88	5	Enviada
540	132	23	2019-01-09 08:20:07-02	0.0199	161533.19	194033.52	32500.33	83185.39	2	2	Enviada
1227	58	23	2016-04-05 11:42:14-03	0.0141	100950.48	188223.70	87273.22	2478.37	61	0	Enviada
1337	647	23	2014-07-05 21:41:42-03	0.0240	80642.27	161191.34	80549.07	2162.66	95	0	Enviada
448	789	24	2017-11-11 03:58:58-02	0.0173	32661.58	50022.95	17361.37	833.86	66	4	Enviada
1327	493	24	2014-10-23 16:49:28-02	0.0105	119643.05	148560.12	28917.07	2445.98	69	4	Enviada
1409	95	24	2011-06-20 22:03:38-03	0.0179	36968.35	44757.84	7789.49	788.56	103	4	Enviada
1579	243	24	2015-03-12 16:56:53-03	0.0152	52774.42	67777.71	15003.29	1875.35	37	5	Enviada
165	651	25	2011-11-07 01:16:25-02	0.0222	188359.28	232132.96	43773.68	7268.74	39	3	Enviada
1608	984	25	2022-06-05 13:09:28-03	0.0195	140012.87	177600.69	37587.82	3818.48	65	0	Enviada
1766	827	25	2016-08-26 21:32:46-03	0.0129	171210.94	216152.24	44941.30	3120.19	96	0	Enviada
1238	626	26	2021-08-17 11:36:04-03	0.0122	111520.32	192191.31	80670.99	2011.95	93	2	Enviada
1518	504	26	2022-03-19 07:28:19-03	0.0177	133231.81	164079.95	30848.14	3622.38	60	5	Enviada
1718	379	26	2011-08-21 14:37:22-03	0.0117	162981.10	207719.18	44738.08	3057.87	84	4	Enviada
1739	317	26	2012-06-22 12:23:21-03	0.0203	25262.26	29962.47	4700.21	708.64	64	3	Enviada
109	902	27	2013-10-18 03:45:28-03	0.0250	155048.82	208071.45	53022.63	9580.05	21	1	Enviada
567	93	27	2017-07-08 16:14:41-03	0.0103	174401.49	211234.22	36832.73	3410.46	73	0	Enviada
598	430	27	2022-11-24 09:17:31-03	0.0230	33410.55	44022.51	10611.96	1374.77	36	1	Enviada
660	897	27	2017-09-06 21:35:08-03	0.0144	33846.45	64823.80	30977.35	1011.29	46	4	Enviada
730	897	27	2016-04-29 03:19:53-03	0.0249	75215.69	129792.02	54576.33	1993.64	114	4	Enviada
1214	371	27	2019-08-13 22:41:27-03	0.0115	88692.00	149447.05	60755.05	2342.33	50	5	Enviada
1370	808	27	2013-08-10 10:42:59-03	0.0223	147410.70	193379.48	45968.78	4259.02	67	5	Enviada
1457	770	27	2014-07-03 21:46:18-03	0.0194	16914.96	21365.48	4450.52	467.48	63	3	Enviada
1477	624	27	2016-07-27 23:44:22-03	0.0161	10901.78	15435.03	4533.25	212.13	110	5	Enviada
1489	584	27	2019-09-22 01:04:37-03	0.0163	18896.79	26491.41	7594.62	570.62	48	5	Enviada
1547	638	27	2017-01-07 04:23:03-02	0.0233	44760.81	61835.32	17074.51	1541.62	49	5	Enviada
1820	358	27	2022-11-26 06:59:44-03	0.0245	21065.28	25728.28	4663.00	587.64	87	0	Enviada
1889	436	27	2016-08-17 12:37:28-03	0.0207	75825.31	113859.74	38034.43	9320.83	9	6	Enviada
1975	643	27	2015-06-05 02:54:51-03	0.0182	100319.06	180144.45	79825.39	26231.19	4	5	Enviada
432	384	28	2017-06-15 17:54:41-03	0.0148	80997.74	149598.05	68600.31	1966.89	64	5	Enviada
513	82	28	2019-01-30 05:56:41-02	0.0129	122515.33	219782.20	97266.87	3205.50	53	2	Enviada
580	35	29	2017-04-17 17:55:15-03	0.0149	166473.24	229462.99	62989.75	5885.49	37	0	Enviada
605	20	29	2014-08-17 02:37:13-03	0.0083	82690.20	120311.64	37621.44	1394.23	82	3	Enviada
895	674	29	2020-07-24 12:00:23-03	0.0210	94843.85	140271.31	45427.46	2363.48	89	1	Enviada
922	327	29	2010-08-16 19:49:10-03	0.0229	159260.11	223323.45	64063.34	10014.52	20	2	Enviada
935	593	30	2018-05-25 00:45:13-03	0.0192	166721.96	233975.80	67253.84	3923.06	89	4	Enviada
1411	107	30	2014-05-15 22:07:04-03	0.0170	74705.40	118591.27	43885.87	1590.69	95	4	Enviada
1679	845	30	2021-12-05 03:09:15-03	0.0146	15546.42	20258.27	4711.85	597.04	33	0	Enviada
1859	506	30	2016-02-10 07:05:00-02	0.0182	102314.92	179501.93	77187.01	4830.16	27	5	Enviada
428	737	31	2015-11-08 02:06:23-02	0.0219	102033.68	167087.88	65054.20	4374.93	33	3	Enviada
442	255	31	2014-09-30 11:54:07-03	0.0235	36427.42	71192.80	34765.38	931.88	108	6	Enviada
1132	819	31	2017-05-07 20:00:05-03	0.0212	100487.26	124271.71	23784.45	9572.20	12	4	Enviada
1404	424	31	2021-08-11 12:33:28-03	0.0099	72467.68	90672.57	18204.89	1386.05	74	0	Enviada
1793	587	31	2013-05-23 11:26:35-03	0.0215	110456.16	135753.05	25296.89	4019.99	42	1	Enviada
1876	769	31	2015-08-18 23:41:58-03	0.0082	74808.24	91810.14	17001.90	1012.53	114	1	Enviada
32	684	32	2019-05-19 07:55:50-03	0.0177	150541.81	214230.17	63688.36	12234.65	14	2	Enviada
841	266	32	2022-11-25 01:38:38-03	0.0220	97160.59	175982.06	78821.47	3297.90	48	3	Enviada
1074	574	32	2019-01-27 05:14:07-02	0.0096	20823.66	38488.39	17664.73	851.67	28	3	Enviada
1118	776	32	2017-10-21 21:50:07-02	0.0100	34437.44	47305.91	12868.47	1563.69	25	2	Enviada
1170	523	32	2011-04-10 05:39:58-03	0.0111	36747.68	50821.87	14074.19	591.44	106	4	Enviada
1616	15	32	2016-11-22 01:43:25-02	0.0225	3962.74	4673.56	710.82	248.23	20	4	Enviada
1655	469	32	2014-05-23 09:01:59-03	0.0087	200491.47	244528.99	44037.52	3295.17	87	0	Enviada
1893	752	32	2012-03-30 00:11:02-03	0.0116	103142.28	124541.11	21398.83	1712.54	104	0	Enviada
1969	292	32	2016-07-11 21:18:43-03	0.0223	125222.82	211143.14	85920.32	8931.16	17	3	Enviada
784	516	33	2011-10-26 15:14:53-02	0.0111	80533.99	151718.99	71185.00	1410.46	91	4	Enviada
1099	621	33	2015-10-10 16:35:33-03	0.0157	11569.14	19598.82	8029.68	258.26	78	3	Enviada
1512	895	33	2020-11-19 10:33:14-03	0.0179	99892.97	174480.38	74587.41	5156.65	24	3	Enviada
1983	254	33	2013-05-10 05:36:58-03	0.0184	113321.08	146660.88	33339.80	5883.37	24	4	Enviada
64	137	34	2019-10-06 18:06:21-03	0.0114	8110.36	11113.79	3003.43	1406.17	6	6	Enviada
381	36	34	2014-11-25 00:21:19-02	0.0115	26168.87	32190.33	6021.46	449.06	97	4	Enviada
424	499	34	2013-06-06 16:59:26-03	0.0165	110943.31	133251.92	22308.61	3512.31	45	1	Enviada
548	460	34	2010-08-01 20:14:29-03	0.0081	42091.20	84055.68	41964.48	1385.95	35	5	Enviada
912	602	34	2017-08-03 17:08:25-03	0.0206	5554.13	8454.51	2900.38	175.04	52	6	Enviada
704	985	35	2012-10-13 13:59:01-03	0.0131	23099.80	29012.83	5913.03	671.76	46	1	Enviada
1269	406	35	2016-12-02 03:25:31-02	0.0126	17118.33	25249.57	8131.24	776.22	26	2	Enviada
149	672	36	2013-01-12 18:16:52-02	0.0191	4958.79	5938.39	979.60	107.65	112	4	Enviada
956	551	36	2017-11-22 08:39:06-02	0.0153	37977.11	48504.95	10527.84	693.12	120	3	Enviada
1066	534	36	2019-11-22 16:41:04-03	0.0104	21706.49	42951.18	21244.69	1399.66	17	6	Enviada
1145	849	36	2021-01-10 22:56:28-03	0.0202	134658.77	175034.36	40375.59	3358.82	83	5	Enviada
200	772	37	2019-03-27 18:33:20-03	0.0183	46919.15	60001.44	13082.29	988.28	112	1	Enviada
516	644	37	2019-08-10 23:24:45-03	0.0218	91145.04	110898.60	19753.56	2260.02	98	0	Enviada
588	699	37	2019-08-02 00:10:15-03	0.0106	136688.45	243963.78	107275.33	2364.14	90	6	Enviada
972	711	37	2010-06-23 04:04:18-03	0.0221	105666.05	159567.04	53900.99	2840.34	79	6	Enviada
982	936	37	2018-04-02 19:24:01-03	0.0138	49850.37	65947.41	16097.04	1071.13	75	4	Enviada
1067	986	37	2022-09-04 01:31:22-03	0.0093	104304.80	190050.37	85745.57	1740.94	88	1	Enviada
1322	128	37	2013-12-09 01:13:48-02	0.0180	134672.44	193374.09	58701.65	2796.61	113	3	Enviada
1781	578	37	2018-11-06 20:04:53-02	0.0241	58488.39	116856.02	58367.63	2896.48	28	5	Enviada
1814	870	37	2012-05-18 14:16:23-03	0.0127	56948.17	81043.30	24095.13	1035.46	95	3	Enviada
1842	214	37	2022-02-22 10:33:14-03	0.0201	98835.31	146726.00	47890.69	4650.26	28	6	Enviada
1932	410	37	2016-08-22 07:08:51-03	0.0211	138943.56	199461.47	60517.91	3291.60	106	1	Enviada
824	843	38	2011-08-08 21:28:33-03	0.0208	72284.28	126768.15	54483.87	4644.51	19	2	Enviada
162	289	39	2018-11-25 00:54:13-02	0.0200	32620.62	46158.58	13537.96	1783.31	23	6	Enviada
215	245	39	2021-02-28 15:17:15-03	0.0103	64763.51	108663.15	43899.64	1023.15	103	1	Enviada
681	986	39	2020-06-24 15:38:26-03	0.0112	79704.96	108450.48	28745.52	3950.19	23	3	Enviada
1462	252	39	2022-01-10 18:42:55-03	0.0090	115885.32	210424.71	94539.39	3971.63	34	5	Enviada
1764	828	39	2021-12-30 12:55:20-03	0.0161	53741.01	76298.13	22557.12	1029.22	115	2	Enviada
182	27	40	2019-12-16 08:31:22-03	0.0089	90173.76	127367.03	37193.27	3541.70	29	2	Enviada
1254	607	40	2014-02-11 09:29:35-02	0.0215	21075.99	39323.96	18247.97	1009.70	28	2	Enviada
1260	662	40	2012-12-10 17:58:43-02	0.0092	46674.15	90265.84	43591.69	1646.36	33	4	Enviada
1870	786	40	2021-07-04 17:22:20-03	0.0236	67383.15	99727.99	32344.84	2849.99	35	2	Enviada
691	713	41	2012-08-16 11:41:07-03	0.0080	141939.18	245172.53	103233.35	2949.74	61	0	Enviada
1124	144	41	2016-02-18 05:47:02-02	0.0226	3067.28	4776.17	1708.89	172.48	23	5	Enviada
1314	93	41	2010-01-26 05:02:23-02	0.0183	27087.45	37943.10	10855.65	603.46	95	1	Enviada
1638	784	41	2010-07-18 15:46:14-03	0.0186	154093.17	224203.36	70110.19	3901.09	72	2	Enviada
1650	638	41	2018-11-29 14:22:41-02	0.0210	71752.24	104875.69	33123.45	1645.55	119	6	Enviada
1686	800	41	2018-05-06 16:19:27-03	0.0111	177277.25	232496.10	55218.85	3047.44	94	3	Enviada
325	290	42	2012-06-04 18:12:18-03	0.0127	59307.65	81807.65	22500.00	1767.72	44	4	Enviada
338	712	42	2020-10-02 09:27:15-03	0.0229	80924.74	130001.32	49076.58	2678.34	52	2	Enviada
1177	828	42	2016-09-01 22:17:34-03	0.0145	36533.84	60981.72	24447.88	3617.14	11	5	Enviada
584	355	43	2022-01-02 03:37:34-03	0.0180	50411.29	95061.64	44650.35	1668.45	44	1	Enviada
1009	472	43	2021-04-14 20:45:03-03	0.0121	92799.68	110491.76	17692.08	4646.42	23	5	Enviada
1246	736	43	2020-03-15 18:31:47-03	0.0116	188058.53	241025.27	52966.74	4419.38	59	4	Enviada
1570	118	43	2013-01-17 02:29:17-02	0.0176	188145.67	237620.42	49474.75	8126.01	30	5	Enviada
1759	42	43	2021-01-19 06:33:14-03	0.0235	164590.18	199065.77	34475.59	8780.73	25	5	Enviada
1818	807	43	2015-04-22 12:43:04-03	0.0234	99051.99	173346.11	74294.12	13720.43	8	2	Enviada
599	425	44	2011-04-30 01:56:48-03	0.0080	169146.01	241497.60	72351.59	2209.01	119	3	Enviada
213	543	45	2017-08-19 07:27:16-03	0.0137	119207.20	161992.55	42785.35	2217.60	98	6	Enviada
313	410	45	2010-12-26 10:16:43-02	0.0243	53872.91	98115.43	44242.52	1521.56	82	4	Enviada
323	995	45	2017-05-06 11:13:49-03	0.0212	79058.02	122381.21	43323.19	1916.17	99	3	Enviada
725	74	45	2013-10-30 05:25:35-02	0.0164	34692.12	50829.70	16137.58	904.14	61	3	Enviada
854	69	45	2011-10-17 03:46:43-02	0.0088	142570.85	249417.73	106846.88	4531.34	37	6	Enviada
1038	869	45	2021-10-11 18:46:17-03	0.0242	110637.04	151119.98	40482.94	5006.87	32	6	Enviada
1055	487	45	2018-01-03 21:02:39-02	0.0113	47878.02	86591.03	38713.01	1222.63	52	3	Enviada
1507	442	45	2016-06-28 00:46:21-03	0.0245	44850.46	74367.28	29516.82	4700.83	11	5	Enviada
1604	951	45	2012-05-08 14:22:56-03	0.0214	156615.47	187324.21	30708.74	4336.57	70	0	Enviada
1635	309	45	2016-01-01 00:48:16-02	0.0174	124807.59	156702.31	31894.72	2767.83	89	1	Enviada
1682	561	45	2012-08-16 04:19:49-03	0.0111	101157.39	141781.50	40624.11	4824.43	24	0	Enviada
610	99	46	2013-06-13 22:53:47-03	0.0222	32896.76	39518.72	6621.96	826.40	98	2	Enviada
767	791	46	2012-04-22 04:35:34-03	0.0167	86229.18	150698.48	64469.30	22464.76	4	4	Enviada
946	600	46	2020-12-26 06:55:29-03	0.0237	10416.91	15278.88	4861.97	311.79	67	3	Enviada
978	659	46	2013-11-19 13:32:03-02	0.0146	123652.70	229884.11	106231.41	2958.55	65	2	Enviada
1475	645	46	2012-03-24 23:52:19-03	0.0229	112197.95	134935.01	22737.06	2845.62	103	3	Enviada
1936	143	46	2017-09-13 20:31:27-03	0.0172	68323.02	84116.03	15793.01	2297.82	42	5	Enviada
427	671	47	2016-07-18 03:24:40-03	0.0085	161195.63	219424.00	58228.37	3165.60	67	6	Enviada
882	165	47	2013-01-05 14:19:41-02	0.0182	130289.14	158741.84	28452.70	15825.61	9	4	Enviada
437	646	48	2017-06-30 21:57:57-03	0.0223	102471.71	139788.66	37316.95	3315.15	53	1	Enviada
1597	256	48	2011-02-10 19:05:16-02	0.0123	31638.70	48318.00	16679.30	3381.83	10	5	Enviada
1994	222	48	2012-03-10 19:40:21-03	0.0146	182080.64	225724.15	43643.51	3505.26	98	3	Enviada
491	193	49	2014-12-18 15:39:14-02	0.0153	69834.69	126972.97	57138.28	1335.56	106	2	Enviada
629	872	49	2016-11-16 13:09:53-02	0.0214	45029.86	58377.64	13347.78	1198.33	77	1	Enviada
973	754	49	2018-09-21 08:46:41-03	0.0107	112058.13	216639.65	104581.52	2161.79	76	6	Enviada
1164	276	49	2022-09-15 01:18:16-03	0.0178	81768.64	163246.11	81477.47	1709.82	108	1	Enviada
1186	922	49	2012-06-22 09:24:15-03	0.0149	103774.64	126481.67	22707.03	1904.25	113	4	Enviada
1205	439	49	2011-06-09 01:17:45-03	0.0191	173133.52	236271.15	63137.63	4536.42	69	4	Enviada
1354	712	49	2016-11-30 18:34:04-02	0.0171	89012.96	158863.72	69850.76	2088.01	77	3	Enviada
1921	51	49	2022-02-20 16:00:42-03	0.0160	107165.07	165158.72	57993.65	2212.16	94	3	Enviada
1548	965	50	2020-12-21 16:50:19-03	0.0102	75743.01	100845.96	25102.95	1971.82	49	6	Enviada
170	23	51	2015-09-14 00:02:11-03	0.0187	76505.07	94013.94	17508.87	16170.00	5	2	Enviada
272	912	51	2011-10-13 20:37:57-03	0.0156	27855.31	51968.60	24113.29	516.38	119	0	Enviada
472	806	51	2011-04-20 22:58:34-03	0.0105	102638.22	124608.80	21970.58	5046.32	23	4	Enviada
1094	414	51	2015-01-22 14:32:59-02	0.0087	39981.23	55367.03	15385.80	1040.05	47	6	Enviada
1283	7	51	2019-04-28 18:12:55-03	0.0135	115084.66	190093.65	75008.99	2907.46	57	2	Enviada
1600	498	51	2017-05-13 09:26:48-03	0.0129	139121.30	219039.21	79917.91	2285.58	120	3	Enviada
1749	417	51	2011-08-25 23:37:13-03	0.0137	171358.66	238501.49	67142.83	4037.84	64	1	Enviada
804	734	52	2021-08-19 01:07:12-03	0.0115	57709.83	68976.53	11266.70	1385.89	57	5	Enviada
818	34	52	2012-06-11 09:58:01-03	0.0116	29117.43	42525.57	13408.14	469.90	110	4	Enviada
920	911	52	2020-09-25 04:08:27-03	0.0103	42931.19	73757.17	30825.98	624.91	120	4	Enviada
1581	581	52	2014-07-19 04:38:52-03	0.0139	67933.35	115153.35	47220.00	1453.27	76	6	Enviada
1644	982	52	2022-04-18 10:58:35-03	0.0153	176429.51	236059.85	59630.34	3518.36	96	2	Enviada
1757	83	52	2015-09-02 19:18:36-03	0.0237	32836.10	53486.63	20650.53	1242.94	42	2	Enviada
54	261	53	2016-01-30 12:30:43-02	0.0182	72148.10	133986.48	61838.38	1490.53	118	6	Enviada
206	520	53	2016-06-12 22:20:17-03	0.0145	144955.26	212496.89	67541.63	2742.63	101	1	Enviada
788	118	53	2021-09-18 08:00:39-03	0.0224	16507.03	30524.88	14017.85	1307.85	15	2	Enviada
897	876	53	2019-10-02 11:10:40-03	0.0249	71852.57	129520.15	57667.58	2554.60	49	1	Enviada
1020	264	53	2010-08-07 00:50:13-03	0.0126	100570.21	186753.92	86183.71	1765.71	101	2	Enviada
1036	9	53	2021-02-12 12:45:40-03	0.0094	7407.43	9572.69	2165.26	218.55	41	4	Enviada
1245	716	53	2022-01-12 04:49:01-03	0.0153	163396.47	218434.06	55037.59	7218.41	28	0	Enviada
1424	124	53	2011-08-17 01:00:58-03	0.0086	157556.26	229150.72	71594.46	11993.37	14	2	Enviada
1826	616	53	2010-09-04 03:46:21-03	0.0142	152168.84	183687.90	31519.06	4213.62	51	5	Enviada
1931	899	53	2011-04-28 09:55:00-03	0.0238	37488.76	74713.76	37225.00	1028.25	86	6	Enviada
357	213	54	2014-09-28 03:01:14-03	0.0098	127569.62	150784.84	23215.22	3142.96	52	5	Enviada
418	53	54	2015-06-06 06:13:32-03	0.0145	39243.68	55878.50	16634.82	705.79	114	0	Enviada
643	885	54	2022-01-21 09:56:10-03	0.0216	118694.57	202025.12	83330.55	4327.61	42	5	Enviada
1194	963	54	2011-03-12 20:33:39-03	0.0139	94802.43	129005.63	34203.20	1760.46	100	6	Enviada
1504	485	54	2010-06-19 05:17:58-03	0.0230	186991.78	243210.81	56219.03	4714.57	107	4	Enviada
1797	637	54	2018-03-09 21:49:24-03	0.0128	81512.82	154692.24	73179.42	1611.17	82	6	Enviada
1905	273	54	2012-05-11 12:06:03-03	0.0172	81239.86	143280.23	62040.37	1845.42	83	6	Enviada
95	660	55	2021-03-10 03:50:09-03	0.0158	60076.66	74316.24	14239.58	1847.46	46	3	Enviada
975	684	55	2020-02-05 20:42:49-03	0.0188	21900.04	26250.95	4350.91	877.60	34	0	Enviada
1062	923	55	2013-12-16 10:48:43-02	0.0091	36855.89	64696.45	27840.56	9424.54	4	1	Enviada
1198	288	55	2015-08-27 05:08:58-03	0.0200	53603.50	101218.96	47615.46	3575.47	18	0	Enviada
1294	965	55	2020-01-03 03:53:28-03	0.0142	59734.19	87687.59	27953.40	5449.18	12	6	Enviada
1902	519	55	2022-11-16 23:28:58-03	0.0178	196985.26	247231.97	50246.71	19876.50	11	3	Enviada
82	129	56	2021-11-24 07:37:20-03	0.0108	145593.87	244313.43	98719.56	2794.40	77	0	Enviada
456	859	56	2010-11-06 04:17:38-02	0.0112	9816.42	16567.10	6750.68	211.21	66	3	Enviada
1217	844	56	2021-04-28 18:00:18-03	0.0108	190603.27	224866.82	34263.55	6014.52	39	2	Enviada
79	114	57	2015-12-17 13:37:23-02	0.0107	87239.71	123207.06	35967.35	1591.27	83	2	Enviada
224	697	57	2016-08-11 08:35:34-03	0.0154	92541.68	142749.33	50207.65	1776.78	106	3	Enviada
537	823	57	2022-10-23 17:14:45-03	0.0124	16507.53	29357.06	12849.53	897.69	21	5	Enviada
1658	193	57	2013-12-26 05:07:03-02	0.0084	29155.84	49624.09	20468.25	398.45	114	3	Enviada
66	854	58	2010-06-15 18:18:53-03	0.0085	92200.26	154975.23	62774.97	1492.23	88	0	Enviada
219	567	58	2018-10-24 14:30:48-03	0.0111	67254.87	80356.64	13101.77	1209.44	87	6	Enviada
388	53	58	2012-03-13 20:08:10-03	0.0233	90272.28	112630.73	22358.45	2658.53	68	2	Enviada
932	475	58	2022-02-14 16:07:13-03	0.0176	88283.09	117348.32	29065.23	2254.13	67	4	Enviada
1987	584	58	2012-06-12 22:31:10-03	0.0094	128475.84	156285.99	27810.15	3868.38	40	6	Enviada
550	129	59	2014-12-06 12:46:00-02	0.0206	133679.14	249763.13	116083.99	3515.57	75	5	Enviada
1732	897	59	2015-03-31 21:18:46-03	0.0099	127680.76	163284.53	35603.77	3881.21	40	5	Enviada
1733	893	59	2013-01-26 11:05:13-02	0.0136	59498.98	112145.98	52647.00	1270.47	75	2	Enviada
1856	849	59	2022-09-12 21:56:34-03	0.0243	102305.04	158163.32	55858.28	2768.98	95	0	Enviada
62	264	60	2012-06-28 07:33:35-03	0.0161	140136.89	184848.44	44711.55	12033.42	13	6	Enviada
106	114	60	2021-01-27 00:13:52-03	0.0091	95446.24	131196.90	35750.66	1495.19	96	3	Enviada
1680	576	60	2019-09-03 14:41:19-03	0.0192	113297.73	189031.05	75733.32	2422.57	120	1	Enviada
133	89	61	2013-02-06 04:47:15-02	0.0086	17764.23	35204.53	17440.30	575.87	36	5	Enviada
736	425	61	2019-02-04 00:14:27-02	0.0112	126647.97	223793.09	97145.12	2386.74	81	0	Enviada
1209	463	61	2011-04-05 20:47:32-03	0.0111	62508.06	88360.64	25852.58	1524.62	55	4	Enviada
1299	229	61	2013-02-15 09:53:06-02	0.0117	123947.87	184731.32	60783.45	4059.17	38	1	Enviada
1589	311	61	2015-01-05 10:26:37-02	0.0145	165366.86	208017.36	42650.50	4805.96	48	0	Enviada
1623	788	61	2013-03-02 23:24:28-03	0.0182	139271.38	181642.02	42370.64	2891.60	116	1	Enviada
1831	837	61	2020-10-27 14:59:31-03	0.0198	67658.76	132181.85	64523.09	8281.33	9	4	Enviada
1857	795	61	2020-01-22 23:09:28-03	0.0213	102778.60	126171.01	23392.41	2548.07	93	0	Enviada
1317	752	62	2012-02-29 23:57:23-03	0.0119	51392.65	102016.69	50624.04	52004.22	1	3	Enviada
1342	129	62	2022-01-28 16:41:42-03	0.0106	178222.69	238769.73	60547.04	3584.81	71	1	Enviada
1460	302	62	2021-10-22 03:18:32-03	0.0198	161765.46	197885.02	36119.56	3603.91	112	1	Enviada
739	651	63	2017-03-01 04:28:46-03	0.0227	33044.31	52467.39	19423.08	8735.16	4	6	Enviada
741	790	63	2022-02-18 12:22:05-03	0.0152	86135.10	135081.00	48945.90	2295.51	56	0	Enviada
782	957	63	2010-01-31 11:19:05-02	0.0120	2806.88	5295.01	2488.13	158.69	20	1	Enviada
1264	187	63	2016-02-03 01:32:06-02	0.0244	131337.58	221227.00	89889.42	7294.87	24	4	Enviada
1355	790	63	2019-04-28 19:47:14-03	0.0237	156092.67	198307.89	42215.22	19462.99	9	6	Enviada
239	130	64	2021-06-17 03:07:59-03	0.0128	41822.96	70311.97	28489.01	776.21	92	0	Enviada
370	883	64	2020-07-26 12:26:19-03	0.0147	77630.22	131066.34	53436.12	1880.00	64	0	Enviada
965	561	64	2019-04-03 15:27:40-03	0.0088	105272.70	138722.78	33450.08	3133.58	40	3	Enviada
1224	304	64	2015-10-28 16:05:21-02	0.0231	113166.51	203313.53	90147.02	2903.33	101	2	Enviada
1901	478	64	2012-08-31 09:42:50-03	0.0198	152575.42	245759.12	93183.70	3473.00	104	6	Enviada
1221	833	65	2015-11-16 01:13:32-02	0.0120	148787.58	212724.26	63936.68	3492.96	60	4	Enviada
1822	553	65	2016-02-14 22:01:02-02	0.0208	103721.61	135291.33	31569.72	2380.51	115	6	Enviada
1851	971	65	2013-02-23 03:49:28-03	0.0205	101945.44	143036.28	41090.84	2658.54	76	6	Enviada
1896	564	65	2017-07-26 01:20:18-03	0.0084	98056.87	136656.48	38599.61	5346.72	20	3	Enviada
482	512	66	2016-02-07 06:24:49-02	0.0092	11079.03	20365.44	9286.41	219.89	68	4	Enviada
860	464	66	2017-11-28 04:45:07-02	0.0162	22076.46	27852.07	5775.61	647.61	50	3	Enviada
1310	380	66	2022-08-12 03:14:09-03	0.0172	47722.81	80115.51	32392.70	1014.93	97	6	Enviada
1423	379	66	2011-02-26 21:03:16-03	0.0167	174870.84	237069.16	62198.32	3977.65	80	1	Enviada
1538	925	66	2013-07-15 11:40:07-03	0.0083	93043.10	140029.33	46986.23	1610.50	79	2	Enviada
1711	640	66	2021-10-27 07:13:35-03	0.0249	120893.69	195580.94	74687.25	3231.63	109	3	Enviada
1805	188	66	2017-09-30 03:09:43-03	0.0239	132617.54	171432.42	38814.88	10624.57	15	0	Enviada
1930	131	66	2010-06-21 22:51:55-03	0.0080	123850.28	241343.23	117492.95	1687.72	111	3	Enviada
21	393	67	2016-10-14 01:52:38-03	0.0089	35952.40	56942.45	20990.05	570.02	93	6	Enviada
509	846	67	2016-02-03 16:04:11-02	0.0190	16784.99	30270.81	13485.82	489.53	56	1	Enviada
1029	505	67	2012-12-17 22:49:39-02	0.0147	68002.72	101077.39	33074.67	1713.53	60	2	Enviada
1309	428	67	2012-02-24 01:46:10-02	0.0102	51894.05	79197.45	27303.40	1133.49	62	5	Enviada
1614	175	67	2014-02-13 17:47:49-02	0.0175	162254.93	228789.70	66534.77	4835.63	51	2	Enviada
150	894	68	2019-12-28 03:30:06-03	0.0244	79253.65	140411.41	61157.76	4042.04	27	3	Enviada
1713	890	68	2022-01-26 06:46:22-03	0.0204	140006.35	227540.41	87534.06	3967.93	63	2	Enviada
1790	751	68	2017-06-26 18:46:25-03	0.0195	44649.40	67807.04	23157.64	5457.20	9	1	Enviada
50	638	69	2020-01-13 18:31:38-03	0.0099	105866.55	146396.58	40530.03	1546.06	115	5	Enviada
522	406	69	2015-08-13 17:46:27-03	0.0145	16451.61	32541.69	16090.08	996.84	19	0	Enviada
564	120	69	2016-11-13 03:53:13-02	0.0175	122112.43	155647.34	33534.91	4129.91	42	3	Enviada
798	481	69	2011-04-30 03:14:34-03	0.0113	182890.06	220982.08	38092.02	3759.73	71	6	Enviada
1008	419	69	2013-09-10 01:12:25-03	0.0086	79156.38	108353.50	29197.12	1128.18	108	5	Enviada
125	162	70	2016-11-01 17:16:02-02	0.0209	172127.84	231919.58	59791.74	12134.55	17	0	Enviada
498	930	70	2015-03-10 11:20:42-03	0.0196	80208.79	123827.47	43618.68	1936.98	86	1	Enviada
722	667	70	2019-08-19 13:16:46-03	0.0168	9807.05	16436.54	6629.49	667.98	17	3	Enviada
926	6	70	2013-04-08 05:49:11-03	0.0143	103685.74	159630.45	55944.71	2803.76	53	3	Enviada
1701	520	70	2017-07-27 09:57:36-03	0.0206	115989.26	228157.36	112168.10	3357.18	61	4	Enviada
204	71	71	2012-07-10 19:04:23-03	0.0247	40042.40	75600.76	35558.36	8611.56	5	4	Enviada
402	381	71	2016-01-03 19:46:32-02	0.0249	149254.43	187154.59	37900.16	4093.09	97	2	Enviada
1240	473	71	2022-12-13 20:09:30-03	0.0135	133818.69	161455.28	27636.59	7360.85	21	0	Enviada
1422	933	71	2020-01-27 16:26:17-03	0.0084	111889.34	201769.91	89880.57	1571.19	109	6	Enviada
1443	214	71	2017-07-28 23:45:57-03	0.0214	160636.69	204113.58	43476.89	6115.54	39	0	Enviada
301	915	72	2014-02-17 19:43:51-03	0.0230	44531.83	57906.49	13374.66	1689.15	41	4	Enviada
398	633	72	2020-11-08 17:26:11-03	0.0203	74174.61	140472.94	66298.33	10138.66	8	5	Enviada
519	236	72	2020-07-30 21:05:45-03	0.0117	151129.51	211434.59	60305.08	3333.12	65	1	Enviada
683	962	72	2019-06-05 15:19:13-03	0.0136	70895.40	121907.06	51011.66	2226.87	42	3	Enviada
794	132	72	2019-12-10 07:06:24-03	0.0160	136153.25	166874.28	30721.03	3620.24	58	2	Enviada
1129	39	72	2016-09-08 04:30:40-03	0.0124	157306.03	218078.88	60772.85	2675.04	106	1	Enviada
1242	309	72	2016-07-21 20:37:00-03	0.0116	6546.07	10427.00	3880.93	121.53	85	1	Enviada
1250	559	72	2020-09-10 06:32:33-03	0.0144	9183.70	18148.50	8964.80	204.13	73	6	Enviada
1662	693	72	2021-03-13 22:34:29-03	0.0199	205244.56	247930.69	42686.13	5942.47	59	4	Enviada
1775	416	72	2019-01-13 19:40:21-02	0.0198	85339.45	113864.48	28525.03	1883.46	116	1	Enviada
94	17	73	2021-01-15 04:58:32-03	0.0248	49088.57	88730.23	39641.66	1288.98	118	2	Enviada
210	28	73	2010-09-02 19:06:54-03	0.0212	68031.96	110541.92	42509.96	1910.89	67	6	Enviada
296	625	73	2013-02-17 11:51:41-03	0.0140	119989.83	181040.55	61050.72	2469.68	82	2	Enviada
374	897	73	2011-05-21 12:12:26-03	0.0246	97996.83	173856.17	75859.34	2732.69	88	6	Enviada
726	67	73	2014-01-31 13:33:17-02	0.0112	136732.65	197682.41	60949.76	2094.01	118	3	Enviada
780	483	73	2016-11-29 13:38:23-02	0.0093	139143.85	211893.29	72749.44	4181.60	40	3	Enviada
1166	11	73	2021-08-24 16:21:46-03	0.0135	129204.21	155483.49	26279.28	2434.44	94	0	Enviada
1219	999	73	2015-06-07 04:19:10-03	0.0247	58949.57	83283.82	24334.25	2025.59	52	4	Enviada
1306	757	73	2013-05-20 15:22:36-03	0.0225	134448.05	211844.48	77396.43	4368.34	53	1	Enviada
1447	698	73	2012-06-19 14:42:04-03	0.0154	64261.64	127423.73	63162.09	6395.51	11	2	Enviada
1542	215	73	2013-05-13 05:42:24-03	0.0120	76574.24	94102.40	17528.16	1216.65	118	0	Enviada
1924	631	73	2012-10-08 02:25:29-03	0.0102	131550.98	183577.94	52026.96	4193.42	38	6	Enviada
76	474	74	2020-07-23 03:34:35-03	0.0123	94007.18	140410.74	46403.56	2010.81	70	3	Enviada
586	341	74	2022-10-05 12:58:02-03	0.0189	24768.39	39369.35	14600.96	538.02	109	0	Enviada
1501	879	74	2017-11-30 19:46:42-02	0.0112	120345.37	226771.35	106425.98	6202.25	22	1	Enviada
1683	251	74	2016-10-08 12:46:43-03	0.0242	170155.87	226373.23	56217.36	7545.27	33	1	Enviada
107	703	75	2020-02-12 10:44:17-03	0.0179	19079.46	28902.62	9823.16	1926.29	11	0	Enviada
135	198	75	2010-05-16 14:50:14-03	0.0132	115471.13	215832.01	100360.88	2329.52	81	0	Enviada
706	588	75	2012-05-07 08:44:10-03	0.0222	70318.91	90079.20	19760.29	2425.16	47	4	Enviada
814	843	75	2020-12-21 16:30:57-03	0.0194	79857.24	106282.27	26425.03	2207.04	63	4	Enviada
1587	323	75	2017-03-19 04:51:54-03	0.0237	134154.77	184008.83	49854.06	4657.53	49	0	Enviada
1972	153	75	2010-09-11 23:15:48-03	0.0127	112790.10	149166.67	36376.57	8844.78	14	0	Enviada
27	37	76	2013-10-09 11:52:53-03	0.0162	16308.76	21412.72	5103.96	798.54	25	4	Enviada
122	277	76	2017-01-23 11:46:43-02	0.0191	171734.75	213147.52	41412.77	26443.35	7	6	Enviada
549	164	76	2021-07-29 16:38:28-03	0.0243	183072.46	227912.76	44840.30	8129.81	33	5	Enviada
756	672	76	2016-05-20 01:16:11-03	0.0122	66486.74	92072.13	25585.39	1710.82	53	2	Enviada
816	420	76	2019-03-16 12:51:17-03	0.0247	110094.24	210320.45	100226.21	6135.30	24	1	Enviada
1681	818	76	2022-09-28 03:11:04-03	0.0176	69681.57	120595.76	50914.19	70907.96	1	2	Enviada
147	462	77	2017-10-20 22:59:14-02	0.0233	131609.78	173677.47	42067.69	3485.26	92	5	Enviada
186	448	77	2017-09-07 02:13:53-03	0.0129	183192.02	245847.82	62655.80	47284.46	4	4	Enviada
667	849	77	2020-12-24 04:58:24-03	0.0137	9038.79	11752.05	2713.26	1579.52	6	3	Enviada
863	4	77	2018-07-18 13:48:03-03	0.0149	31531.84	51599.21	20067.37	649.08	87	3	Enviada
1184	426	77	2019-04-18 17:56:19-03	0.0176	42785.37	82958.29	40172.92	1262.69	52	6	Enviada
1303	308	77	2020-06-25 20:58:31-03	0.0248	180256.61	240219.34	59962.73	12012.31	19	5	Enviada
1785	454	77	2017-03-21 14:15:13-03	0.0120	125501.51	223824.71	98323.20	3753.22	43	4	Enviada
1828	484	77	2017-08-02 17:57:23-03	0.0241	12955.49	22548.88	9593.39	378.82	73	0	Enviada
39	715	78	2014-12-09 23:04:25-02	0.0225	149351.26	178107.43	28756.17	4162.60	74	1	Enviada
58	921	78	2022-06-12 04:34:46-03	0.0211	119231.09	147606.68	28375.59	5162.08	32	0	Enviada
63	233	78	2013-09-21 06:52:40-03	0.0209	65921.45	102903.99	36982.54	1535.56	110	5	Enviada
132	38	78	2015-02-23 00:32:57-03	0.0108	40526.31	53534.35	13008.04	1796.12	26	6	Enviada
451	140	78	2022-02-10 12:12:56-03	0.0145	16256.33	28980.79	12724.46	1145.75	16	4	Enviada
1268	182	78	2018-09-15 18:03:51-03	0.0090	124531.38	232512.49	107981.11	3103.91	50	2	Enviada
78	222	79	2011-03-24 02:11:45-03	0.0247	3807.65	6850.73	3043.08	477.02	9	1	Enviada
285	798	79	2021-03-18 20:57:36-03	0.0166	69718.93	130839.39	61120.46	70876.26	1	1	Enviada
700	916	79	2021-07-17 13:52:24-03	0.0229	92476.93	119740.36	27263.43	2291.13	114	5	Enviada
769	640	79	2022-07-09 11:07:17-03	0.0246	90570.19	115394.90	24824.71	2369.39	116	4	Enviada
1191	927	79	2016-11-09 22:22:29-02	0.0208	23677.21	42446.92	18769.71	998.84	33	1	Enviada
1459	385	79	2014-07-24 23:47:05-03	0.0138	35237.55	59888.77	24651.22	740.53	78	3	Enviada
1535	236	79	2011-06-30 03:43:51-03	0.0151	155986.40	188190.55	32204.15	10472.43	17	5	Enviada
1780	250	79	2011-06-17 05:24:14-03	0.0169	117286.89	201012.83	83725.94	2385.95	106	5	Enviada
891	720	80	2012-04-14 20:58:24-03	0.0126	95054.24	127680.52	32626.28	2293.13	59	5	Enviada
1256	31	80	2020-03-12 22:54:16-03	0.0205	86785.20	159337.72	72552.52	2548.91	59	1	Enviada
1383	229	80	2012-06-13 12:24:02-03	0.0245	111232.03	171795.41	60563.38	3483.30	63	2	Enviada
1877	707	80	2020-01-09 23:35:05-03	0.0162	135066.41	246043.60	110977.19	2567.36	119	3	Enviada
98	287	81	2015-09-19 04:30:25-03	0.0085	34476.89	51516.23	17039.34	530.41	95	0	Enviada
187	434	81	2013-06-07 02:52:56-03	0.0130	14794.21	25661.59	10867.38	269.24	97	5	Enviada
530	903	81	2010-09-06 17:30:39-03	0.0191	115185.50	158235.29	43049.79	2836.29	79	4	Enviada
757	760	81	2015-11-26 08:08:54-02	0.0092	48837.36	96935.61	48098.25	778.43	94	1	Enviada
758	480	81	2011-11-17 06:58:14-02	0.0144	94836.95	131026.64	36189.69	6018.65	18	1	Enviada
960	405	81	2017-01-06 02:09:59-02	0.0092	96396.57	187942.11	91545.54	4048.32	27	0	Enviada
1081	192	81	2016-09-28 08:48:55-03	0.0115	45401.10	73863.63	28462.53	1298.05	45	5	Enviada
19	885	82	2019-05-03 08:58:12-03	0.0192	93312.72	176013.40	82700.68	3762.43	34	0	Enviada
562	217	82	2016-03-06 16:16:38-03	0.0195	137343.39	191042.55	53699.16	8359.00	20	2	Enviada
1444	77	82	2014-08-03 14:23:00-03	0.0109	122820.61	164298.71	41478.10	2239.69	84	5	Enviada
1590	450	82	2010-01-08 04:53:18-02	0.0127	22086.76	35165.82	13079.06	707.67	40	0	Enviada
1736	843	82	2013-08-22 07:24:07-03	0.0203	75089.47	88448.20	13358.73	2351.19	52	5	Enviada
1792	862	82	2012-01-23 06:40:47-02	0.0169	146702.80	215055.03	68352.23	3086.72	97	1	Enviada
1830	987	82	2013-06-03 21:12:28-03	0.0143	32040.12	54868.77	22828.65	1853.35	20	6	Enviada
1229	40	83	2010-05-31 19:21:07-03	0.0092	130016.76	238018.03	108001.27	4708.93	32	4	Enviada
1292	765	83	2011-08-17 03:18:12-03	0.0248	84574.48	104042.73	19468.25	29601.21	3	0	Enviada
1387	700	83	2021-08-17 20:05:09-03	0.0117	163244.59	203494.77	40250.18	4941.90	42	2	Enviada
1521	221	83	2017-11-12 09:17:28-02	0.0118	6775.98	10732.24	3956.26	325.85	24	1	Enviada
254	357	84	2012-02-29 16:53:11-03	0.0163	132400.72	181398.56	48997.84	2973.91	80	2	Enviada
327	931	84	2018-03-22 22:07:42-03	0.0130	88211.73	149873.73	61662.00	1755.48	82	0	Enviada
976	650	84	2013-09-25 23:27:55-03	0.0172	93482.84	162417.86	68935.02	2559.97	58	2	Enviada
1727	964	84	2016-01-26 16:02:16-02	0.0236	103497.56	122189.85	18692.29	10792.90	11	2	Enviada
1791	150	84	2016-02-20 08:19:34-02	0.0148	12435.58	20470.95	8035.37	328.20	56	0	Enviada
1847	841	84	2020-08-16 14:07:17-03	0.0125	109073.28	174923.80	65850.52	5107.08	25	1	Enviada
1981	956	84	2013-05-27 11:52:33-03	0.0206	111381.12	166256.27	54875.15	2579.66	108	6	Enviada
92	26	85	2016-12-21 23:31:23-02	0.0096	74090.50	112484.19	38393.69	1610.40	61	5	Enviada
496	154	85	2021-03-11 11:54:22-03	0.0099	30205.04	44958.33	14753.29	498.42	93	5	Enviada
1110	559	85	2012-11-06 04:03:56-02	0.0219	116305.09	212517.42	96212.33	4886.55	34	4	Enviada
1350	138	85	2021-06-16 18:54:08-03	0.0113	97368.23	185316.96	87948.73	6686.44	16	4	Enviada
1431	632	85	2016-08-23 06:34:29-03	0.0131	101033.29	175702.67	74669.38	2017.47	82	3	Enviada
1563	624	85	2015-04-13 09:39:18-03	0.0158	100596.54	121817.81	21221.27	2690.28	57	3	Enviada
117	199	86	2017-08-05 11:43:50-03	0.0195	127990.13	174164.23	46174.10	3028.35	90	1	Enviada
697	324	86	2020-11-21 10:37:07-03	0.0196	81725.16	162457.02	80731.86	1870.30	100	1	Enviada
925	532	86	2013-03-11 18:41:55-03	0.0231	141250.58	230607.85	89357.27	4792.90	50	5	Enviada
934	247	86	2020-10-11 13:34:11-03	0.0190	71362.77	134563.94	63201.17	2081.30	56	1	Enviada
1134	493	86	2012-12-05 20:55:43-02	0.0084	65569.51	113743.79	48174.28	16738.06	4	6	Enviada
157	432	87	2014-12-04 23:58:14-02	0.0163	34704.05	61034.51	26330.46	2047.36	20	4	Enviada
1028	208	87	2020-02-06 04:39:40-03	0.0126	192003.04	238886.79	46883.75	13325.61	16	0	Enviada
1049	223	87	2011-09-04 10:40:23-03	0.0089	52143.49	89547.76	37404.27	4997.19	11	2	Enviada
1330	287	87	2020-01-14 23:41:31-03	0.0216	112921.21	150691.15	37769.94	3757.91	49	0	Enviada
1362	147	87	2016-09-19 16:23:45-03	0.0182	16001.80	30296.83	14295.03	458.06	56	4	Enviada
1619	695	87	2010-05-11 03:36:10-03	0.0248	51313.70	71639.69	20325.99	1597.61	65	2	Enviada
572	321	88	2017-03-18 18:27:10-03	0.0142	13142.16	22951.98	9809.82	1565.87	9	4	Enviada
637	665	88	2021-11-11 20:21:55-03	0.0126	57570.49	109616.72	52046.23	1079.63	89	5	Enviada
1783	440	88	2017-10-03 09:01:46-03	0.0088	180693.96	237827.14	57133.18	2981.16	87	5	Enviada
1853	406	88	2017-05-20 19:11:10-03	0.0199	52364.34	102554.87	50190.53	1477.53	62	1	Enviada
1979	789	88	2018-05-18 19:57:27-03	0.0135	33037.58	53902.25	20864.67	6877.51	5	3	Enviada
383	805	89	2015-08-14 15:21:44-03	0.0211	98749.20	157298.85	58549.65	2941.80	59	5	Enviada
500	347	89	2022-06-17 07:33:09-03	0.0231	15038.36	19967.63	4929.27	435.42	70	0	Enviada
526	739	89	2022-03-19 02:01:19-03	0.0198	77244.24	152592.44	75348.20	2022.36	72	1	Enviada
762	472	89	2022-01-11 05:47:38-03	0.0104	63875.53	126975.98	63100.45	1152.70	83	5	Enviada
1544	677	89	2018-09-28 16:30:19-03	0.0236	80281.84	138404.47	58122.63	2555.13	58	5	Enviada
1866	889	89	2017-02-12 13:40:40-02	0.0191	2919.50	5704.98	2785.48	255.74	13	1	Enviada
334	807	90	2022-01-06 15:12:05-03	0.0121	1701.52	2056.17	354.65	96.30	20	3	Enviada
1552	609	90	2017-05-26 09:20:42-03	0.0182	69729.76	98243.68	28513.92	70998.84	1	5	Enviada
1628	692	90	2015-12-23 19:17:21-02	0.0151	49577.64	95954.47	46376.83	2567.56	23	5	Enviada
1809	836	90	2015-05-27 17:56:31-03	0.0216	63240.41	81591.10	18350.69	1503.26	112	3	Enviada
1869	195	90	2021-09-14 00:27:55-03	0.0082	33978.13	57497.35	23519.22	2149.43	17	1	Enviada
557	491	91	2016-01-05 14:12:40-02	0.0109	61066.32	118844.09	57777.77	1263.76	69	5	Enviada
1691	154	91	2020-12-28 22:20:14-03	0.0175	43602.63	56330.60	12727.97	15045.85	3	3	Enviada
1772	841	91	2019-11-17 15:57:13-03	0.0238	50251.52	69195.26	18943.74	5246.22	11	4	Enviada
4	454	92	2015-02-28 14:16:17-03	0.0220	159503.55	230634.56	71131.01	5179.59	52	6	Enviada
34	863	92	2019-03-12 01:55:52-03	0.0132	143220.32	187987.07	44766.75	3120.33	71	2	Enviada
247	330	92	2019-05-06 03:15:15-03	0.0219	4192.06	8346.53	4154.47	350.92	14	5	Enviada
821	385	92	2011-07-28 01:02:11-03	0.0186	167617.88	241756.20	74138.32	5000.59	53	4	Enviada
5	535	93	2019-12-04 21:14:07-03	0.0235	69165.19	106962.78	37797.59	2129.95	62	2	Enviada
802	244	93	2016-12-15 17:47:29-02	0.0217	126030.01	178749.29	52719.28	3000.06	113	4	Enviada
955	694	93	2012-08-25 19:08:05-03	0.0178	25248.93	31977.76	6728.83	1932.82	15	6	Enviada
1226	706	93	2011-04-24 00:09:54-03	0.0196	127090.66	240764.57	113673.91	5383.98	32	4	Enviada
1882	937	93	2012-11-16 03:58:12-02	0.0148	120882.69	226711.54	105828.85	3698.50	45	2	Enviada
14	610	94	2019-07-11 10:28:03-03	0.0218	139410.47	203931.20	64520.73	5028.45	43	3	Enviada
318	787	94	2011-11-06 19:52:59-02	0.0165	82631.65	139219.34	56587.69	4197.54	24	6	Enviada
529	989	94	2012-07-21 09:12:22-03	0.0096	130082.86	201668.56	71585.70	2898.11	59	5	Enviada
1546	579	94	2015-11-21 17:02:10-02	0.0175	5799.40	10184.94	4385.54	1221.48	5	0	Enviada
1917	351	94	2013-10-16 11:57:35-03	0.0166	148751.31	241262.01	92510.70	4099.98	56	6	Enviada
845	261	95	2016-02-10 17:00:41-02	0.0113	111264.40	159029.10	47764.70	1748.43	113	6	Enviada
1388	651	95	2016-02-12 17:39:52-02	0.0226	53598.77	67640.63	14041.86	1384.59	93	0	Enviada
1417	308	95	2014-02-20 00:19:33-03	0.0135	30035.87	38245.11	8209.24	506.89	120	5	Enviada
1685	429	95	2019-02-28 20:32:16-03	0.0145	140067.95	185209.06	45141.11	3758.37	54	2	Enviada
1702	764	95	2022-06-21 21:00:17-03	0.0240	144333.30	201185.56	56852.26	4231.11	72	5	Enviada
1742	569	95	2014-07-21 21:48:43-03	0.0237	87748.67	166304.05	78555.38	2306.56	99	1	Enviada
1961	162	95	2016-10-24 09:34:01-02	0.0129	127250.24	173349.14	46098.90	2113.22	117	6	Enviada
113	176	96	2021-12-09 21:25:09-03	0.0221	124489.84	232380.19	107890.35	4051.17	52	6	Enviada
221	662	96	2022-11-02 21:49:59-03	0.0141	19232.11	30765.39	11533.28	366.83	96	3	Enviada
268	211	96	2021-11-19 05:13:59-03	0.0203	106093.33	172061.77	65968.44	2487.04	100	4	Enviada
377	771	96	2020-07-24 20:51:36-03	0.0100	34762.03	65705.60	30943.57	5998.13	6	0	Enviada
724	856	96	2014-03-07 06:18:05-03	0.0130	57382.28	90788.13	33405.85	1448.89	56	3	Enviada
849	215	96	2013-11-21 16:23:17-02	0.0190	37589.10	45424.71	7835.61	812.95	112	3	Enviada
1039	290	96	2014-06-01 19:09:39-03	0.0219	97385.14	171930.90	74545.76	2785.07	67	1	Enviada
1762	411	96	2014-02-04 19:28:55-02	0.0181	82020.39	158702.58	76682.19	4729.75	21	3	Enviada
638	766	97	2013-01-24 18:33:43-02	0.0232	12020.60	20327.29	8306.69	330.43	81	3	Enviada
1019	862	97	2022-01-07 04:17:19-03	0.0090	103602.21	182932.70	79330.49	3095.69	40	6	Enviada
1024	354	97	2019-06-09 11:46:50-03	0.0168	42934.83	59069.10	16134.27	2051.78	26	4	Enviada
1482	38	97	2015-02-16 13:56:08-02	0.0087	8105.08	15004.54	6899.46	144.87	77	1	Enviada
471	944	98	2012-08-22 12:32:00-03	0.0163	986.59	1859.96	873.37	19.16	113	3	Enviada
1266	422	98	2019-03-15 09:38:45-03	0.0123	83621.21	157453.14	73831.93	1436.29	103	0	Enviada
1942	628	98	2011-01-14 04:49:53-02	0.0142	129951.85	154763.42	24811.57	5657.22	28	4	Enviada
1955	688	98	2011-03-17 08:16:20-03	0.0175	115367.77	219963.56	104595.79	3357.77	53	2	Enviada
288	684	99	2010-06-03 01:04:33-03	0.0211	154699.67	216491.47	61791.80	5157.05	48	5	Enviada
450	936	99	2010-04-13 18:22:16-03	0.0219	62883.74	109115.39	46231.65	2591.05	35	1	Enviada
458	492	99	2022-07-18 04:39:52-03	0.0115	21600.01	40310.37	18710.36	753.15	35	0	Enviada
682	397	99	2010-03-26 16:07:16-03	0.0175	154850.56	187560.90	32710.34	3445.58	89	5	Enviada
917	366	99	2012-07-13 12:53:52-03	0.0161	54342.75	97311.34	42968.59	1379.12	63	4	Enviada
1115	380	99	2017-10-05 15:40:10-03	0.0166	94737.13	182392.49	87655.36	2611.20	56	2	Enviada
1607	130	99	2020-08-28 23:35:37-03	0.0230	14037.56	19633.56	5596.00	1007.01	17	0	Enviada
1664	805	99	2015-11-24 21:41:06-02	0.0206	148664.91	209612.60	60947.69	5245.01	43	6	Enviada
1688	889	99	2014-05-16 20:57:47-03	0.0233	101556.81	162054.91	60498.10	2836.82	78	2	Enviada
1724	673	99	2012-05-15 08:46:09-03	0.0185	62123.72	119646.36	57522.64	1613.05	68	0	Enviada
1765	17	99	2016-06-11 23:31:57-03	0.0143	66588.87	119811.49	53222.62	5282.15	14	0	Enviada
151	579	100	2012-05-10 05:11:49-03	0.0084	34995.30	59994.20	24998.90	1079.55	38	0	Enviada
561	48	100	2019-08-11 20:25:23-03	0.0223	51135.35	82532.59	31397.24	1265.18	105	0	Enviada
750	708	100	2010-12-01 20:55:57-02	0.0180	2620.73	3465.50	844.77	67.64	67	6	Enviada
967	757	100	2018-05-20 15:15:28-03	0.0167	147484.47	188587.30	41102.83	3396.12	78	5	Enviada
1053	852	100	2014-03-12 18:45:07-03	0.0149	35324.85	53339.03	18014.18	2497.75	16	4	Enviada
1204	553	100	2018-01-02 15:13:49-02	0.0211	11807.62	15875.49	4067.87	296.32	88	3	Enviada
1371	237	100	2012-06-09 14:41:36-03	0.0212	69396.06	138626.51	69230.45	1649.71	106	0	Enviada
1776	22	100	2014-07-16 21:32:18-03	0.0214	83846.90	124477.48	40630.58	2537.39	58	5	Enviada
3	215	1	2018-04-08 07:47:24-03	0.0238	154691.56	214508.76	59817.20	6687.29	34	4	Aprovada
372	193	1	2010-03-04 16:43:32-03	0.0103	128429.43	248837.17	120407.74	2846.10	61	3	Aprovada
669	82	1	2010-11-21 11:14:49-02	0.0196	148806.31	234174.16	85367.85	3383.88	102	3	Aprovada
1837	341	1	2015-03-07 02:40:35-03	0.0138	161162.29	228485.39	67323.10	3701.83	67	6	Aprovada
214	502	2	2012-09-25 08:50:19-03	0.0097	76659.01	133066.12	56407.11	2188.84	43	6	Aprovada
596	266	2	2022-07-05 14:39:02-03	0.0242	131492.08	206480.64	74988.56	45969.01	3	5	Aprovada
1021	790	2	2010-03-29 14:44:55-03	0.0093	83080.83	140314.25	57233.42	7980.75	11	4	Aprovada
1156	669	2	2022-11-03 02:06:09-03	0.0201	7234.21	10905.53	3671.32	210.46	59	6	Aprovada
1735	725	2	2016-11-21 05:39:18-02	0.0088	86125.76	143359.53	57233.77	1323.79	97	3	Aprovada
337	566	3	2022-12-18 17:55:58-03	0.0080	69848.71	127319.85	57471.14	2628.11	30	4	Aprovada
563	435	3	2014-12-29 04:35:43-02	0.0183	98018.24	163214.64	65196.40	3602.03	38	5	Aprovada
938	154	3	2014-02-18 15:05:26-03	0.0242	188729.37	242187.95	53458.58	6137.94	57	0	Aprovada
1499	986	3	2011-11-23 06:50:33-02	0.0090	9742.68	14569.05	4826.37	134.36	118	6	Aprovada
1500	673	3	2017-07-25 08:03:15-03	0.0237	153458.11	186909.25	33451.14	4167.44	88	1	Aprovada
1804	659	3	2012-12-17 02:00:05-02	0.0143	110462.59	187926.74	77464.15	3645.43	40	3	Aprovada
152	569	4	2015-09-03 14:27:44-03	0.0100	102555.08	151511.08	48956.00	5683.12	20	2	Aprovada
785	644	5	2021-01-05 09:43:09-03	0.0111	100312.23	142248.96	41936.73	1768.19	90	6	Aprovada
1185	535	5	2017-08-01 06:47:51-03	0.0225	121875.12	145161.31	23286.19	16816.89	8	5	Aprovada
1391	740	5	2021-01-28 04:57:09-03	0.0193	10855.04	17090.85	6235.81	255.17	90	3	Aprovada
1908	380	5	2012-11-13 01:46:19-02	0.0239	2709.30	3343.00	633.70	96.58	47	4	Aprovada
614	826	6	2010-08-27 14:11:37-03	0.0121	44364.98	53280.04	8915.06	811.83	90	6	Aprovada
1386	897	6	2018-08-22 16:58:57-03	0.0088	47721.43	63892.83	16171.40	2738.49	19	0	Aprovada
1958	643	6	2015-12-28 20:10:38-02	0.0091	158198.87	190935.60	32736.73	2975.52	73	1	Aprovada
13	112	7	2021-08-21 00:04:37-03	0.0217	944.01	1652.35	708.34	22.34	116	6	Aprovada
899	186	7	2018-07-07 01:36:14-03	0.0151	81940.15	127497.71	45557.56	2230.04	54	4	Aprovada
1069	304	7	2022-01-22 19:34:40-03	0.0164	101745.77	142646.60	40900.83	2052.98	103	6	Aprovada
1800	382	7	2021-08-23 10:43:00-03	0.0153	79373.67	138677.79	59304.12	27271.60	3	4	Aprovada
265	481	8	2019-09-23 21:31:23-03	0.0127	2721.37	3718.44	997.07	52.19	86	4	Aprovada
666	626	8	2013-04-16 06:22:43-03	0.0081	16701.13	21132.31	4431.18	452.74	44	4	Aprovada
101	827	9	2015-12-29 13:40:59-02	0.0179	23780.28	38218.77	14438.49	516.43	98	3	Aprovada
746	92	9	2011-12-01 01:32:56-02	0.0246	75780.24	108197.34	32417.10	2053.98	98	4	Aprovada
1705	585	9	2020-04-30 16:05:21-03	0.0245	104978.22	157068.88	52090.66	2843.76	97	0	Aprovada
164	59	10	2015-08-04 05:33:35-03	0.0213	71884.51	123637.15	51752.64	2299.75	52	6	Aprovada
379	797	10	2022-05-15 22:00:15-03	0.0186	30567.29	57420.05	26852.76	702.26	90	2	Aprovada
417	126	10	2020-04-25 15:08:17-03	0.0249	133251.20	192758.12	59506.92	3889.02	78	0	Aprovada
453	586	10	2013-09-16 21:09:56-03	0.0129	77458.86	146368.08	68909.22	2000.44	54	1	Aprovada
479	160	10	2012-04-03 10:54:13-03	0.0115	106241.69	129159.36	22917.67	2070.41	78	4	Aprovada
542	344	10	2019-01-18 08:44:28-02	0.0197	64202.22	118357.50	54155.28	1817.78	61	0	Aprovada
585	433	10	2012-05-30 09:54:53-03	0.0161	43340.19	68132.12	24791.93	871.41	101	3	Aprovada
1485	534	10	2012-06-16 10:42:10-03	0.0193	23033.62	34129.87	11096.25	781.60	44	0	Aprovada
1496	844	10	2011-09-28 21:33:30-03	0.0102	83829.25	150136.05	66306.80	1247.27	114	0	Aprovada
1529	64	10	2015-08-24 10:23:03-03	0.0172	125748.13	228795.46	103047.33	2756.97	90	4	Aprovada
1567	322	10	2018-06-16 19:36:34-03	0.0099	99162.61	123829.91	24667.30	5489.60	20	1	Aprovada
1629	989	10	2015-07-04 03:01:40-03	0.0111	93795.29	147038.79	53243.50	1517.18	105	5	Aprovada
558	304	11	2012-08-08 22:20:06-03	0.0093	50138.23	64454.53	14316.30	2639.31	21	6	Aprovada
1007	477	11	2019-06-28 20:35:10-03	0.0195	21308.41	38455.19	17146.78	715.60	45	0	Aprovada
1348	166	11	2011-06-29 23:33:11-03	0.0085	187368.86	247972.70	60603.84	7795.66	27	3	Aprovada
1698	974	11	2010-07-02 01:01:02-03	0.0134	102374.26	181186.45	78812.19	1830.28	104	0	Aprovada
401	381	12	2018-09-18 01:40:05-03	0.0209	153936.07	220403.96	66467.89	14636.71	12	5	Aprovada
364	124	13	2018-01-15 22:59:56-02	0.0089	118005.01	140398.05	22393.04	3137.42	46	5	Aprovada
680	305	13	2010-05-13 11:01:20-03	0.0142	125941.00	152157.33	26216.33	32610.86	4	2	Aprovada
1109	868	13	2013-09-06 10:39:17-03	0.0232	82474.21	104582.66	22108.45	4384.72	25	3	Aprovada
1176	185	13	2012-05-22 12:58:45-03	0.0225	45004.70	87601.63	42596.93	1212.58	81	6	Aprovada
1323	250	13	2022-06-27 18:44:35-03	0.0208	35073.08	61732.16	26659.08	886.86	84	5	Aprovada
1338	89	13	2016-05-26 07:10:01-03	0.0168	1544.23	2686.85	1142.62	42.31	57	2	Aprovada
148	977	14	2016-05-03 04:39:34-03	0.0185	21409.03	33942.43	12533.40	509.37	82	3	Aprovada
390	731	14	2016-08-13 06:15:47-03	0.0221	44905.60	82678.99	37773.39	1585.16	45	6	Aprovada
597	278	14	2014-02-13 03:32:13-02	0.0155	153339.02	184273.02	30934.00	6114.36	32	4	Aprovada
1033	317	14	2020-11-10 23:05:05-03	0.0125	95334.49	131387.49	36053.00	1620.64	107	0	Aprovada
1855	326	14	2021-12-17 05:09:29-03	0.0246	77958.77	132052.36	54093.59	2386.11	67	0	Aprovada
1962	325	14	2013-12-21 13:52:35-02	0.0231	96168.13	181788.59	85620.46	49756.52	2	3	Aprovada
1977	795	14	2016-01-23 17:08:53-02	0.0246	149018.71	238371.53	89352.82	7826.38	26	4	Aprovada
477	42	15	2013-12-23 00:18:55-02	0.0242	127838.93	186532.25	58693.32	9261.93	17	6	Aprovada
879	940	15	2013-05-13 08:59:03-03	0.0216	31742.21	55718.26	23976.05	789.28	95	3	Aprovada
1308	390	15	2021-03-15 14:13:43-03	0.0097	38900.42	57310.90	18410.48	1070.92	45	4	Aprovada
1568	775	15	2011-09-11 16:27:48-03	0.0204	142948.14	237373.46	94425.32	3247.66	113	0	Aprovada
376	772	16	2016-02-23 18:43:40-03	0.0081	132611.90	249769.01	117157.11	3184.55	51	0	Aprovada
924	84	16	2017-12-21 16:48:42-02	0.0097	102240.19	160986.94	58746.75	1565.31	104	1	Aprovada
948	283	16	2021-08-30 02:16:24-03	0.0204	16374.68	25075.10	8700.42	567.37	44	3	Aprovada
962	72	16	2020-10-10 13:15:03-03	0.0185	1649.37	2180.78	531.41	42.21	70	1	Aprovada
1135	658	16	2019-01-17 17:51:56-02	0.0108	53655.20	84533.02	30877.82	27262.99	2	4	Aprovada
422	767	17	2011-05-13 16:45:21-03	0.0177	108673.57	192219.73	83546.16	5417.15	25	2	Aprovada
1290	701	17	2020-02-25 03:37:03-03	0.0092	42093.66	65760.97	23667.31	670.94	94	6	Aprovada
1912	740	17	2016-05-26 14:59:59-03	0.0159	161208.88	217634.61	56425.73	7865.12	25	0	Aprovada
1923	283	17	2015-12-08 20:10:31-02	0.0122	90461.32	123554.84	33093.52	3833.41	28	4	Aprovada
56	692	18	2019-12-27 19:55:29-03	0.0126	37945.86	64943.69	26997.83	2792.17	15	3	Aprovada
476	255	18	2022-06-18 20:21:12-03	0.0228	54108.81	73240.14	19131.33	2635.71	28	1	Aprovada
503	445	18	2014-02-09 08:10:20-02	0.0197	22005.79	37966.99	15961.20	3000.12	8	0	Aprovada
608	765	18	2015-03-23 15:00:36-03	0.0212	37628.03	45367.34	7739.31	1362.05	42	0	Aprovada
793	677	18	2021-07-02 05:38:23-03	0.0088	182648.99	227647.20	44998.21	5326.09	41	0	Aprovada
900	29	18	2011-10-18 15:50:52-02	0.0201	32170.92	62254.76	30083.84	2371.27	16	4	Aprovada
1098	456	18	2013-05-25 04:37:01-03	0.0227	73201.54	95229.01	22027.47	1782.23	120	6	Aprovada
1187	86	18	2020-12-17 01:14:39-03	0.0205	127345.90	162276.53	34930.63	3399.15	72	1	Aprovada
1565	954	18	2016-05-29 19:28:51-03	0.0154	130883.38	173686.83	42803.45	4184.57	43	1	Aprovada
1651	701	18	2012-11-28 04:18:36-02	0.0178	146684.84	186257.79	39572.95	3414.54	82	0	Aprovada
36	417	19	2014-11-23 08:33:37-02	0.0109	28777.16	35598.90	6821.74	584.28	71	4	Aprovada
1200	496	19	2015-08-15 01:40:56-03	0.0189	10287.98	13308.56	3020.58	224.11	108	1	Aprovada
1328	483	19	2019-05-26 04:29:16-03	0.0156	25382.65	47916.56	22533.91	1125.82	28	2	Aprovada
345	17	20	2022-11-06 15:59:43-03	0.0115	60101.06	71835.44	11734.38	2003.55	37	6	Aprovada
485	338	20	2021-03-30 05:59:12-03	0.0246	142181.29	213555.40	71374.11	4019.60	84	1	Aprovada
718	8	20	2015-08-31 14:07:51-03	0.0198	2441.85	4169.50	1727.65	53.77	117	5	Aprovada
969	249	20	2022-11-18 23:07:57-03	0.0178	58196.38	78917.69	20721.31	1686.25	54	4	Aprovada
1059	664	20	2015-02-13 06:23:23-02	0.0130	130742.02	241005.07	110263.05	7465.87	20	1	Aprovada
1165	762	20	2021-09-05 07:11:36-03	0.0229	91244.25	174521.30	83277.05	2769.98	62	2	Aprovada
1366	873	20	2015-12-20 15:55:07-02	0.0151	70207.37	86108.79	15901.42	1359.31	101	0	Aprovada
140	383	21	2014-09-26 20:28:51-03	0.0159	124861.41	189588.52	64727.11	2394.46	112	1	Aprovada
867	467	21	2013-11-13 19:05:12-02	0.0100	79939.94	159313.51	79373.57	2386.29	41	3	Aprovada
1158	985	21	2022-02-28 15:30:43-03	0.0189	82772.42	128290.86	45518.44	3322.05	34	4	Aprovada
1265	16	21	2022-01-29 02:42:08-03	0.0222	74746.45	114570.17	39823.72	2024.58	78	1	Aprovada
1456	937	21	2013-05-14 04:59:02-03	0.0206	22199.42	38132.46	15933.04	542.07	91	2	Aprovada
1720	418	21	2011-02-01 16:03:20-02	0.0083	145031.99	234466.30	89434.31	8707.53	18	1	Aprovada
1819	930	21	2011-01-25 14:06:19-02	0.0152	63930.41	76288.84	12358.43	1337.11	86	1	Aprovada
1886	422	21	2019-08-27 07:42:26-03	0.0164	151729.73	234388.14	82658.41	3162.78	95	4	Aprovada
207	799	22	2022-03-21 04:33:13-03	0.0112	161775.67	206714.34	44938.67	3199.69	75	2	Aprovada
404	47	22	2018-10-12 23:02:03-03	0.0113	128596.07	246726.47	118130.40	2431.85	81	5	Aprovada
441	831	22	2012-03-06 18:29:25-03	0.0128	41662.14	49729.71	8067.57	967.40	63	1	Aprovada
1175	230	22	2015-05-01 14:02:44-03	0.0207	26189.27	41339.86	15150.59	600.18	114	0	Aprovada
1693	171	22	2022-06-09 04:29:31-03	0.0172	137832.32	200827.95	62995.63	3895.53	55	2	Aprovada
1900	111	22	2019-10-30 02:15:55-03	0.0153	21407.75	39079.51	17671.76	404.91	109	1	Aprovada
1960	789	22	2020-02-27 08:47:44-03	0.0141	94618.14	139340.00	44721.86	1711.35	108	5	Aprovada
103	545	23	2020-10-12 20:19:47-03	0.0241	59138.72	87311.69	28172.97	2141.25	46	4	Aprovada
212	284	23	2010-04-05 20:29:04-03	0.0140	41714.96	71742.40	30027.44	7297.11	6	2	Aprovada
394	228	23	2022-01-29 15:25:44-03	0.0214	136501.84	215503.84	79002.00	6708.51	27	4	Aprovada
492	934	23	2013-07-20 11:08:38-03	0.0195	50523.56	94411.57	43888.01	1590.97	50	0	Aprovada
644	262	23	2014-05-30 18:29:15-03	0.0187	4841.48	6072.16	1230.68	197.93	33	6	Aprovada
646	865	23	2019-05-28 09:32:52-03	0.0216	56963.56	79022.68	22059.12	1407.51	97	3	Aprovada
1117	163	23	2022-05-30 21:13:10-03	0.0095	122068.51	186353.63	64285.12	18107.28	7	6	Aprovada
1346	564	23	2019-10-16 15:06:37-03	0.0185	1338.95	2252.54	913.59	35.03	67	6	Aprovada
1359	178	23	2020-04-09 17:22:33-03	0.0227	93336.67	172384.14	79047.47	2742.03	66	1	Aprovada
1813	607	23	2010-06-11 18:10:42-03	0.0155	176121.08	249974.50	73853.42	3554.33	95	6	Aprovada
232	31	24	2016-07-11 23:25:24-03	0.0206	6717.59	10247.01	3529.42	216.48	50	6	Aprovada
410	235	24	2010-11-23 11:19:06-02	0.0174	105045.03	195552.16	90507.13	18589.04	6	0	Aprovada
539	66	24	2016-09-05 08:44:56-03	0.0192	119049.78	165184.97	46135.19	2661.01	103	3	Aprovada
844	103	24	2021-03-25 00:03:51-03	0.0244	83759.77	112593.50	28833.73	2245.25	100	3	Aprovada
869	742	24	2010-09-09 03:15:00-03	0.0116	115173.42	156347.13	41173.71	2327.30	74	6	Aprovada
1057	345	24	2018-12-02 09:26:32-02	0.0121	89573.82	133208.94	43635.12	1629.13	91	6	Aprovada
1089	539	24	2022-12-15 00:49:02-03	0.0234	188744.63	235327.59	46582.96	5980.02	58	5	Aprovada
1296	76	24	2019-10-18 13:45:06-03	0.0113	128762.56	212397.11	83634.55	8367.64	17	6	Aprovada
1505	8	24	2010-04-12 08:33:17-03	0.0219	67263.72	114766.43	47502.71	1865.08	72	3	Aprovada
799	618	25	2018-08-23 10:16:19-03	0.0179	2906.22	3695.82	789.60	62.65	100	6	Aprovada
1365	710	25	2020-04-03 22:23:26-03	0.0115	37744.22	69065.94	31321.72	713.39	82	5	Aprovada
1812	507	25	2022-04-13 18:51:20-03	0.0243	142957.99	169267.55	26309.56	9483.70	19	4	Aprovada
1852	129	25	2012-01-25 06:18:28-02	0.0214	50533.37	59810.97	9277.60	1295.62	85	3	Aprovada
1916	649	25	2017-08-01 19:41:54-03	0.0170	185615.91	239372.09	53756.18	39037.74	5	2	Aprovada
1999	172	25	2018-09-23 15:10:17-03	0.0218	175384.05	231785.51	56401.46	8026.03	30	1	Aprovada
551	430	26	2013-07-10 17:41:18-03	0.0106	58362.56	81052.49	22689.93	1023.20	88	6	Aprovada
1626	513	26	2014-06-09 02:47:59-03	0.0086	137529.13	214897.57	77368.44	8730.40	17	2	Aprovada
29	538	27	2017-08-15 11:03:52-03	0.0240	171981.10	246294.43	74313.33	4520.45	103	4	Aprovada
229	84	27	2015-08-30 02:42:17-03	0.0237	118458.24	228609.83	110151.59	3427.41	73	2	Aprovada
796	130	27	2013-11-22 02:33:04-02	0.0245	47842.76	72629.65	24786.89	1498.23	63	6	Aprovada
866	755	27	2018-01-10 08:51:26-02	0.0118	60983.76	73423.36	12439.60	61703.37	1	0	Aprovada
1609	462	27	2014-03-22 14:34:07-03	0.0132	127976.70	199943.62	71966.92	2185.99	113	4	Aprovada
1633	777	27	2020-10-19 08:04:57-03	0.0148	114343.15	184175.48	69832.33	3675.16	42	1	Aprovada
1014	892	28	2013-06-01 13:23:06-03	0.0186	70984.26	128636.52	57652.26	1809.22	71	6	Aprovada
1027	531	28	2016-12-17 10:43:27-02	0.0165	88808.13	155708.91	66900.78	2165.38	69	6	Aprovada
1632	353	28	2014-04-11 11:33:47-03	0.0163	55065.81	82963.36	27897.55	3393.49	19	5	Aprovada
244	897	29	2010-05-06 13:37:46-03	0.0193	166883.00	215072.28	48189.28	3779.63	100	1	Aprovada
415	809	29	2013-10-04 22:52:30-03	0.0147	14248.82	25973.56	11724.74	399.04	51	4	Aprovada
661	322	29	2016-06-07 02:12:20-03	0.0197	27437.23	41036.39	13599.16	951.95	43	6	Aprovada
1490	343	29	2019-04-15 17:24:05-03	0.0128	64058.78	81232.93	17174.15	1536.09	60	2	Aprovada
1844	194	29	2022-07-30 22:48:53-03	0.0183	33538.98	62563.05	29024.07	1005.28	52	3	Aprovada
344	321	30	2016-01-08 11:45:59-02	0.0084	106031.55	131782.86	25751.31	1613.43	96	2	Aprovada
717	745	30	2019-08-31 13:50:05-03	0.0205	160500.60	205409.47	44908.87	4942.34	54	3	Aprovada
1395	113	30	2012-08-21 09:34:22-03	0.0105	174234.15	219599.10	45364.95	3204.52	81	5	Aprovada
1525	38	30	2010-05-11 10:57:51-03	0.0238	82323.93	159786.54	77462.61	2250.03	87	1	Aprovada
517	214	31	2016-12-15 22:37:43-02	0.0118	111572.82	153338.99	41766.17	1756.61	118	0	Aprovada
571	91	31	2013-03-11 22:04:48-03	0.0212	166245.34	234298.09	68052.75	4067.23	96	4	Aprovada
632	27	31	2018-03-08 00:56:29-03	0.0231	51916.82	97820.47	45903.65	1292.81	115	1	Aprovada
1947	25	31	2019-10-13 01:55:16-03	0.0155	38412.70	57038.14	18625.44	925.70	67	2	Aprovada
127	543	32	2010-03-30 16:16:31-03	0.0237	105249.32	190305.51	85056.19	107743.73	1	1	Aprovada
241	622	32	2020-05-29 06:53:02-03	0.0249	138378.45	210778.11	72399.66	4730.20	53	0	Aprovada
532	233	32	2010-06-19 07:22:36-03	0.0201	82398.31	162916.81	80518.50	1957.74	94	3	Aprovada
807	198	32	2019-08-12 07:18:04-03	0.0211	122758.14	144641.60	21883.46	7032.49	22	3	Aprovada
1307	456	32	2010-04-17 17:23:09-03	0.0220	40720.83	55404.42	14683.59	981.65	112	4	Aprovada
1704	469	32	2019-12-28 05:48:19-03	0.0107	88559.03	126308.79	37749.76	4203.61	24	6	Aprovada
1801	245	32	2014-08-15 11:10:10-03	0.0106	113789.99	164380.18	50590.19	2116.79	80	1	Aprovada
330	621	33	2017-11-10 18:03:05-02	0.0164	11082.10	16645.08	5562.98	297.59	58	4	Aprovada
367	119	33	2018-05-20 12:41:42-03	0.0159	146630.94	184393.24	37762.30	3635.27	65	2	Aprovada
611	483	33	2016-07-30 06:42:14-03	0.0119	137328.72	179932.71	42603.99	2381.19	98	3	Aprovada
630	507	33	2018-09-14 04:13:05-03	0.0173	117524.96	153134.66	35609.70	2389.14	111	2	Aprovada
1273	879	33	2017-10-18 21:41:49-02	0.0162	46211.83	73390.74	27178.91	905.77	109	4	Aprovada
1761	754	33	2019-06-28 18:41:31-03	0.0184	99966.57	196939.35	96972.78	5566.62	22	6	Aprovada
30	597	34	2015-06-12 15:00:04-03	0.0198	6994.27	12559.56	5565.29	660.56	12	0	Aprovada
652	820	34	2017-10-20 23:53:05-02	0.0180	134896.61	235065.04	100168.43	3213.10	79	1	Aprovada
910	674	34	2022-04-23 00:15:25-03	0.0236	15301.50	19535.75	4234.25	4053.71	4	3	Aprovada
942	996	34	2017-07-16 22:38:56-03	0.0208	27171.22	39834.81	12663.59	1825.15	18	1	Aprovada
1329	408	34	2012-12-24 13:49:55-02	0.0114	66375.29	93476.75	27101.46	1723.47	51	0	Aprovada
1449	86	34	2010-11-19 20:12:37-02	0.0167	71251.06	96174.12	24923.06	1592.74	83	3	Aprovada
625	928	35	2010-06-07 15:21:16-03	0.0143	96230.70	164544.85	68314.15	16850.67	6	2	Aprovada
773	853	35	2015-10-29 23:22:06-02	0.0152	52439.15	98620.47	46181.32	964.73	116	1	Aprovada
1150	448	35	2011-01-17 19:57:55-02	0.0239	177185.45	228871.58	51686.13	9496.32	25	5	Aprovada
220	348	36	2011-04-20 08:20:44-03	0.0167	140109.12	206360.66	66251.54	2914.90	98	4	Aprovada
531	350	36	2022-07-22 17:55:01-03	0.0219	30191.21	41208.08	11016.87	1294.52	33	2	Aprovada
1228	178	36	2020-07-13 05:18:58-03	0.0175	121580.95	150264.55	28683.60	3423.47	56	6	Aprovada
1448	255	36	2016-07-28 08:52:04-03	0.0126	180840.88	226713.08	45872.20	4413.55	58	0	Aprovada
1887	367	36	2022-08-28 21:32:50-03	0.0213	155841.28	207527.60	51686.32	5492.07	44	5	Aprovada
169	992	37	2017-04-04 06:33:29-03	0.0248	143912.43	179264.45	35352.02	4011.39	90	3	Aprovada
514	720	37	2022-07-07 23:07:37-03	0.0208	175097.39	228404.09	53306.70	4802.24	69	6	Aprovada
688	446	37	2018-10-11 08:22:09-03	0.0115	160417.47	212662.97	52245.50	41263.96	4	5	Aprovada
995	691	37	2015-10-06 18:20:05-03	0.0171	38630.93	75077.50	36446.57	950.72	70	0	Aprovada
1093	53	37	2016-02-03 07:45:56-02	0.0144	22818.67	33318.64	10499.97	692.52	45	5	Aprovada
1316	691	37	2021-06-28 23:13:08-03	0.0159	32267.98	44016.38	11748.40	660.68	95	4	Aprovada
1440	22	37	2015-08-25 03:44:30-03	0.0180	119168.44	150234.62	31066.18	8199.45	17	5	Aprovada
166	797	38	2018-03-09 18:36:48-03	0.0131	29005.84	51685.61	22679.77	2441.14	13	4	Aprovada
264	883	38	2018-11-06 11:26:33-02	0.0110	39658.66	51024.31	11365.65	1439.59	33	0	Aprovada
454	767	38	2021-03-22 14:46:56-03	0.0218	60002.52	117827.92	57825.40	2379.40	37	5	Aprovada
703	508	38	2014-01-19 23:00:47-02	0.0236	120208.59	189100.49	68891.90	4480.11	43	1	Aprovada
994	117	38	2019-03-20 23:52:55-03	0.0137	15485.55	24251.63	8766.08	735.73	25	5	Aprovada
1199	454	38	2015-07-02 21:57:37-03	0.0234	153068.18	186991.14	33922.96	156649.98	1	5	Aprovada
1426	226	38	2014-05-22 20:34:37-03	0.0157	203774.31	243811.26	40036.95	4675.63	74	6	Aprovada
1752	455	38	2021-03-30 19:07:32-03	0.0139	109165.30	204223.76	95058.46	5578.01	23	0	Aprovada
208	551	39	2018-01-02 01:48:45-02	0.0117	42886.30	82355.14	39468.84	768.37	91	6	Aprovada
615	292	39	2016-01-13 05:15:42-02	0.0118	105203.20	130136.29	24933.09	2121.53	75	3	Aprovada
1723	74	39	2019-10-05 12:46:01-03	0.0123	9475.49	11149.75	1674.26	221.74	61	0	Aprovada
1746	599	39	2012-10-05 04:22:25-03	0.0211	10378.52	13825.63	3447.11	364.38	44	1	Aprovada
1834	268	39	2015-10-28 14:09:35-02	0.0113	108807.51	130524.11	21716.60	1821.75	100	3	Aprovada
159	873	40	2011-12-15 20:35:43-02	0.0245	61178.43	74835.28	13656.85	4443.33	17	2	Aprovada
1894	850	40	2015-02-11 15:48:22-02	0.0184	15592.70	19794.48	4201.78	337.59	104	6	Aprovada
429	204	41	2014-01-30 23:43:40-02	0.0098	6991.14	9489.28	2498.14	103.10	112	5	Aprovada
786	957	41	2020-02-19 23:01:51-03	0.0211	94878.85	163065.82	68186.97	2362.75	90	1	Aprovada
826	760	41	2012-04-29 23:24:26-03	0.0149	162257.88	226427.62	64169.74	3054.56	106	5	Aprovada
1461	606	41	2015-10-07 15:41:47-03	0.0107	49796.93	78784.47	28987.54	25298.80	2	1	Aprovada
445	118	42	2020-05-18 19:01:16-03	0.0172	33240.82	40267.83	7027.01	663.51	116	2	Aprovada
1239	27	42	2022-05-06 20:32:01-03	0.0171	21903.30	42712.26	20808.96	453.67	103	6	Aprovada
884	429	43	2016-07-01 02:23:40-03	0.0102	87781.93	169487.11	81705.18	1277.10	119	3	Aprovada
1174	389	43	2018-03-10 18:33:05-03	0.0096	45133.66	53404.00	8270.34	1140.82	50	6	Aprovada
1179	659	43	2013-07-24 00:51:56-03	0.0219	4654.68	7574.38	2919.70	218.53	29	0	Aprovada
1255	636	43	2016-04-30 19:20:22-03	0.0219	118803.36	202872.82	84069.46	3354.10	69	4	Aprovada
1281	30	43	2014-07-12 12:44:07-03	0.0181	138998.52	234244.23	95245.71	8015.43	21	6	Aprovada
1843	877	43	2015-01-31 10:45:59-02	0.0095	14591.99	22214.07	7622.08	320.18	60	6	Aprovada
77	627	44	2014-06-03 23:03:48-03	0.0118	141470.62	214578.34	73107.72	11802.32	13	6	Aprovada
96	220	44	2019-07-16 07:45:50-03	0.0188	159669.25	188629.98	28960.73	6537.49	33	1	Aprovada
119	822	44	2010-01-01 08:18:49-02	0.0181	76158.68	120331.33	44172.65	1830.16	78	6	Aprovada
501	392	44	2016-01-05 23:44:45-02	0.0156	57012.83	85488.95	28476.12	1244.62	81	1	Aprovada
634	696	45	2010-10-18 04:27:39-02	0.0180	57267.22	94125.29	36858.07	1248.06	98	5	Aprovada
1060	973	45	2013-03-03 09:07:42-03	0.0181	55162.33	97950.92	42788.59	1193.40	101	1	Aprovada
1347	116	45	2017-12-22 09:22:34-02	0.0246	168528.93	225773.97	57245.04	5672.93	54	1	Aprovada
1427	483	45	2010-09-15 18:29:54-03	0.0230	79583.05	99754.25	20171.20	2236.81	75	3	Aprovada
343	58	46	2011-09-26 21:04:24-03	0.0216	97817.89	175932.38	78114.49	3183.27	51	3	Aprovada
1054	844	46	2014-07-27 14:14:46-03	0.0096	12756.53	24664.06	11907.53	267.71	64	2	Aprovada
1375	95	46	2022-07-22 17:39:18-03	0.0138	113466.12	158048.23	44582.11	3113.53	51	2	Aprovada
1699	398	46	2016-08-20 08:15:30-03	0.0243	113319.56	180095.69	66776.13	3508.35	64	5	Aprovada
250	794	47	2020-05-19 00:39:01-03	0.0155	148973.40	178047.17	29073.77	11207.23	15	0	Aprovada
1010	430	47	2013-06-05 08:43:21-03	0.0124	14371.03	26635.47	12264.44	576.58	30	3	Aprovada
1410	840	47	2014-06-04 17:30:34-03	0.0154	189484.99	233508.24	44023.25	4861.29	60	6	Aprovada
1549	658	47	2020-11-23 00:50:19-03	0.0247	58686.45	110594.38	51907.93	1631.00	90	6	Aprovada
1989	914	47	2010-12-10 07:41:23-02	0.0196	99503.67	198915.94	99412.27	3003.11	54	2	Aprovada
504	932	48	2013-11-03 20:05:37-02	0.0225	108235.58	149449.25	41213.67	19486.19	6	0	Aprovada
590	443	48	2017-01-01 17:40:05-02	0.0142	77985.92	147389.48	69403.56	16267.87	5	4	Aprovada
952	40	48	2015-05-27 22:04:06-03	0.0169	87516.60	121981.10	34464.50	2403.82	57	5	Aprovada
1104	938	48	2016-09-25 16:11:45-03	0.0089	99604.28	189103.76	89499.48	1776.55	78	3	Aprovada
1435	699	48	2014-03-21 00:08:24-03	0.0209	3659.28	4408.73	749.45	89.56	93	1	Aprovada
1479	934	48	2022-04-06 21:51:09-03	0.0100	34973.73	62997.85	28024.12	878.78	51	5	Aprovada
689	80	49	2011-05-13 13:15:02-03	0.0227	159783.16	201322.86	41539.70	14331.61	13	6	Aprovada
791	542	49	2016-02-29 07:11:10-03	0.0197	125304.94	188872.35	63567.41	64509.87	2	0	Aprovada
861	261	49	2019-08-18 05:14:47-03	0.0186	122367.54	181496.90	59129.36	3234.47	66	3	Aprovada
1321	208	49	2021-04-19 20:12:43-03	0.0144	33511.77	48519.58	15007.81	846.89	59	2	Aprovada
1690	96	49	2018-09-28 20:05:57-03	0.0229	120462.05	240896.16	120434.11	4115.70	49	4	Aprovada
144	962	50	2010-09-18 21:10:09-03	0.0226	2424.35	4049.81	1625.46	84.27	47	4	Aprovada
248	650	50	2013-04-26 09:34:24-03	0.0121	79824.40	125158.15	45333.75	1518.93	84	2	Aprovada
633	856	50	2022-03-09 15:28:51-03	0.0203	18163.39	32862.38	14698.99	439.27	91	5	Aprovada
90	413	51	2017-07-11 07:38:56-03	0.0172	72237.78	110221.47	37983.69	5203.04	16	4	Aprovada
358	813	51	2016-04-29 04:28:44-03	0.0247	48201.85	68406.81	20204.96	1970.07	38	3	Aprovada
690	504	51	2011-09-04 20:17:00-03	0.0227	147949.08	202456.02	54506.94	5424.83	43	2	Aprovada
1408	660	51	2021-08-18 12:58:09-03	0.0224	16288.50	20309.99	4021.49	398.99	111	5	Aprovada
280	442	52	2010-07-02 10:05:30-03	0.0156	93425.41	146359.10	52933.69	2861.27	46	5	Aprovada
293	583	52	2013-07-17 02:07:23-03	0.0154	109223.76	157513.32	48289.56	7351.67	17	0	Aprovada
510	996	52	2021-01-16 02:11:02-03	0.0178	143086.30	176897.30	33811.00	5030.93	40	1	Aprovada
649	608	52	2022-11-22 00:12:33-03	0.0238	77533.55	100233.67	22700.12	2340.98	66	0	Aprovada
1615	52	52	2011-07-23 17:20:19-03	0.0101	114554.68	147967.72	33413.04	2225.76	73	5	Aprovada
1946	246	52	2013-05-23 03:17:38-03	0.0120	96571.14	165860.13	69288.99	1690.29	97	5	Aprovada
203	548	53	2015-06-23 15:00:23-03	0.0239	32485.41	43035.43	10550.02	830.01	116	2	Aprovada
578	274	53	2022-11-03 18:04:14-03	0.0234	98224.97	129024.88	30799.91	2751.37	78	3	Aprovada
620	743	53	2011-05-22 04:28:55-03	0.0135	112992.92	144872.96	31880.04	7113.02	18	0	Aprovada
1142	421	53	2013-11-09 12:44:15-02	0.0221	101099.94	153347.25	52247.31	2758.03	76	5	Aprovada
1467	220	53	2012-03-03 05:40:01-03	0.0134	179635.07	230689.86	51054.79	4755.91	53	6	Aprovada
1640	454	53	2022-11-19 01:22:11-03	0.0196	135476.44	174381.78	38905.34	4225.57	51	0	Aprovada
1802	912	53	2019-05-01 17:23:08-03	0.0087	58051.57	104276.02	46224.45	2504.45	26	4	Aprovada
321	281	54	2017-08-04 05:51:16-03	0.0189	171606.19	247171.16	75564.97	3807.24	102	5	Aprovada
701	927	54	2013-11-07 21:01:46-02	0.0095	94138.12	119018.45	24880.33	2337.55	51	0	Aprovada
748	65	54	2016-02-01 02:21:11-02	0.0236	50208.06	82757.84	32549.78	3309.66	19	5	Aprovada
810	24	54	2018-08-15 23:45:58-03	0.0225	49436.01	71630.49	22194.48	1806.08	43	1	Aprovada
1879	514	54	2018-05-23 10:52:53-03	0.0172	45841.67	74490.50	28648.83	1208.17	62	4	Aprovada
1970	792	54	2022-06-27 20:01:44-03	0.0100	9634.80	18831.94	9197.14	180.02	77	1	Aprovada
335	977	55	2011-07-29 23:30:37-03	0.0244	53713.98	69285.33	15571.35	1450.58	97	3	Aprovada
565	352	55	2012-11-22 11:31:39-02	0.0236	149014.97	190769.05	41754.08	4216.46	77	3	Aprovada
893	842	55	2022-02-28 17:57:09-03	0.0088	129334.12	233774.63	104440.51	4532.89	33	0	Aprovada
913	887	55	2014-02-06 19:23:42-02	0.0212	79994.53	119144.52	39149.99	3720.96	29	3	Aprovada
1571	319	55	2018-11-07 06:20:06-02	0.0207	129851.91	221686.62	91834.71	66948.79	2	5	Aprovada
153	23	56	2016-03-31 14:03:09-03	0.0174	189270.13	247869.01	58598.88	6191.89	44	6	Aprovada
324	638	56	2017-09-18 11:55:21-03	0.0082	56074.17	73991.96	17917.79	2685.09	23	1	Aprovada
642	643	56	2014-06-10 10:50:48-03	0.0240	89445.68	145557.80	56112.12	4664.30	26	6	Aprovada
668	544	56	2015-04-18 22:41:35-03	0.0141	52272.92	88117.90	35844.98	6955.48	8	5	Aprovada
992	704	56	2016-05-11 05:16:01-03	0.0232	63615.74	123319.05	59703.31	2292.70	45	2	Aprovada
1561	169	56	2012-11-21 19:23:06-02	0.0099	170965.97	243971.50	73005.53	3030.36	83	6	Aprovada
1442	335	57	2013-05-30 07:46:12-03	0.0230	15166.13	27079.22	11913.09	440.57	69	3	Aprovada
1495	995	57	2015-02-15 17:55:55-02	0.0105	147908.10	234745.56	86837.46	10709.00	15	4	Aprovada
1594	922	57	2019-12-18 02:30:13-03	0.0195	12038.87	15517.87	3479.00	292.52	84	0	Aprovada
263	29	58	2018-04-09 19:11:42-03	0.0237	78448.06	133189.11	54741.05	14180.38	6	0	Aprovada
408	741	58	2018-02-03 12:26:15-02	0.0129	1491.06	2317.75	826.69	55.77	33	5	Aprovada
1965	403	58	2019-09-30 16:52:26-03	0.0247	117752.15	201644.57	83892.42	6772.18	23	5	Aprovada
981	33	59	2020-08-31 13:52:56-03	0.0096	181646.18	221718.33	40072.15	10025.49	20	2	Aprovada
1492	833	59	2010-01-30 08:07:52-02	0.0198	192837.34	248966.65	56129.31	99291.66	2	0	Aprovada
1551	338	59	2019-06-04 00:50:19-03	0.0209	5777.85	9370.72	3592.87	165.80	63	3	Aprovada
1636	415	59	2011-10-03 19:04:15-03	0.0165	25824.28	43099.78	17275.50	523.03	103	3	Aprovada
2000	629	59	2013-10-25 08:03:04-02	0.0242	55622.23	110099.09	54476.86	1891.59	52	3	Aprovada
545	2	60	2022-04-10 10:08:00-03	0.0111	88190.30	136195.78	48005.48	2045.25	59	3	Aprovada
1363	470	60	2014-06-17 11:21:24-03	0.0212	163743.83	225168.79	61424.96	3846.10	111	5	Aprovada
348	62	61	2015-03-14 19:57:41-03	0.0093	160774.28	215036.91	54262.63	2447.07	102	2	Aprovada
461	478	61	2014-10-20 05:48:25-02	0.0205	123276.63	170674.99	47398.36	3332.22	70	5	Aprovada
711	713	61	2015-07-02 22:29:26-03	0.0194	103711.28	122856.12	19144.84	35920.35	3	0	Aprovada
930	191	61	2013-03-11 20:22:32-03	0.0181	103395.19	123815.11	20419.92	2193.19	107	4	Aprovada
1376	257	61	2022-02-26 18:19:04-03	0.0194	1247.58	2396.47	1148.89	29.07	93	5	Aprovada
236	918	62	2018-03-10 18:11:39-03	0.0237	3035.32	5696.24	2660.92	420.98	8	0	Aprovada
368	999	62	2017-07-21 16:21:28-03	0.0217	57840.79	94803.78	36962.99	1502.72	84	0	Aprovada
819	101	62	2011-03-02 11:15:16-03	0.0166	8295.51	14647.86	6352.35	221.59	59	0	Aprovada
1133	429	62	2021-11-25 08:40:40-03	0.0124	68285.27	125287.26	57001.99	1189.28	101	0	Aprovada
1909	247	62	2011-08-01 06:06:13-03	0.0130	145665.58	218884.27	73218.69	2550.84	105	0	Aprovada
37	257	63	2020-02-23 01:44:29-03	0.0081	97400.34	168604.77	71204.43	2140.29	57	0	Aprovada
217	877	63	2018-07-19 20:35:00-03	0.0167	4764.93	8370.54	3605.61	143.16	49	4	Aprovada
286	729	63	2012-02-05 09:08:38-02	0.0159	110304.51	198967.85	88663.34	2349.40	87	5	Aprovada
391	84	63	2018-05-15 19:10:21-03	0.0194	159475.69	227385.11	67909.42	4742.03	55	6	Aprovada
392	144	63	2013-09-29 21:34:03-03	0.0155	117007.19	194510.23	77503.04	3380.14	50	5	Aprovada
556	822	63	2022-04-02 07:46:39-03	0.0132	72655.00	109363.08	36708.08	1520.17	76	5	Aprovada
560	565	63	2019-12-14 12:29:11-03	0.0114	123478.66	233674.68	110196.02	2022.94	105	0	Aprovada
937	412	63	2020-03-05 02:44:02-03	0.0151	112989.18	178225.37	65236.19	2050.78	119	1	Aprovada
986	111	63	2020-04-30 14:31:33-03	0.0105	59451.95	70673.34	11221.39	4304.51	15	3	Aprovada
1458	667	63	2012-04-04 12:06:18-03	0.0105	154225.19	195648.44	41423.25	3250.98	66	3	Aprovada
1753	714	63	2014-08-19 22:52:14-03	0.0210	63938.03	79170.55	15232.52	2341.34	41	1	Aprovada
67	572	64	2013-03-17 07:13:56-03	0.0081	59681.52	112216.04	52534.52	3992.09	16	5	Aprovada
852	820	64	2016-10-26 21:16:07-02	0.0093	129417.96	205114.45	75696.49	4246.79	36	2	Aprovada
1125	245	64	2014-07-22 03:28:52-03	0.0218	32834.26	58728.52	25894.26	875.06	79	6	Aprovada
1274	604	64	2018-07-13 22:10:45-03	0.0156	49501.00	62283.27	12782.27	4932.16	11	2	Aprovada
249	718	65	2018-05-05 14:46:12-03	0.0166	122100.07	178789.66	56689.59	3613.14	50	6	Aprovada
310	207	65	2020-11-01 20:15:39-03	0.0110	131315.06	234992.19	103677.13	3194.88	55	2	Aprovada
369	592	65	2022-09-05 10:43:27-03	0.0116	147404.48	246291.64	98887.16	4720.30	39	1	Aprovada
436	5	65	2012-08-13 12:40:27-03	0.0196	156438.84	211631.90	55193.06	6781.64	31	0	Aprovada
840	714	65	2022-07-28 15:33:21-03	0.0152	145376.82	198747.21	53370.39	3919.18	55	1	Aprovada
1122	304	65	2013-09-07 16:10:04-03	0.0118	117501.07	146408.91	28907.84	1948.39	106	6	Aprovada
1215	682	65	2011-02-28 02:54:04-03	0.0103	35544.45	46434.71	10890.26	2569.55	15	5	Aprovada
1580	684	65	2010-07-20 14:53:43-03	0.0132	50119.08	95137.69	45018.61	1652.43	39	3	Aprovada
1621	276	65	2021-08-05 12:21:15-03	0.0099	43763.28	63723.70	19960.42	1561.06	33	5	Aprovada
1703	754	65	2019-11-02 11:03:31-03	0.0154	41556.91	66409.58	24852.67	852.05	91	3	Aprovada
1769	160	65	2012-07-28 08:37:56-03	0.0087	8331.17	11999.10	3667.93	1082.58	8	0	Aprovada
228	656	66	2018-08-16 09:06:43-03	0.0204	151818.29	225788.14	73969.85	8063.22	24	2	Aprovada
386	582	66	2017-08-11 06:00:05-03	0.0175	101845.69	140307.85	38462.16	2867.77	56	1	Aprovada
838	639	66	2011-07-24 11:13:27-03	0.0148	178597.23	210754.41	32157.18	3200.32	119	2	Aprovada
902	766	66	2017-12-29 03:32:05-02	0.0121	139842.77	205172.17	65329.40	3088.45	66	3	Aprovada
1304	651	66	2011-05-03 06:27:49-03	0.0082	164457.47	217589.45	53131.98	5424.38	35	1	Aprovada
1513	788	66	2020-01-08 21:19:53-03	0.0242	137192.37	227660.97	90468.60	3621.25	104	0	Aprovada
1613	84	66	2019-04-20 09:35:17-03	0.0113	19932.13	27473.95	7541.82	354.00	90	4	Aprovada
173	664	67	2020-11-13 17:28:15-03	0.0136	5709.75	10584.10	4874.35	123.85	73	3	Aprovada
185	640	67	2021-12-10 23:26:50-03	0.0164	76780.21	142054.13	65273.92	9245.85	9	6	Aprovada
1472	870	67	2017-06-04 07:48:43-03	0.0137	32196.43	56689.92	24493.49	837.19	55	6	Aprovada
246	247	68	2015-07-06 14:09:44-03	0.0196	24942.04	32209.20	7267.16	1739.36	17	5	Aprovada
253	811	68	2022-02-21 03:15:50-03	0.0144	92743.18	113289.30	20546.12	6531.27	16	5	Aprovada
754	436	68	2019-05-20 08:57:34-03	0.0186	13811.40	22282.19	8470.79	2454.05	6	2	Aprovada
822	453	68	2013-11-27 21:42:54-02	0.0130	110471.79	148003.73	37531.94	4040.70	34	3	Aprovada
1430	497	68	2014-04-27 19:06:46-03	0.0142	62921.28	108336.83	45415.55	1901.83	45	2	Aprovada
24	924	69	2015-09-01 21:42:44-03	0.0181	41488.46	76889.12	35400.66	1099.89	64	6	Aprovada
763	503	69	2017-03-28 09:24:27-03	0.0097	85762.09	120050.10	34288.01	1453.43	88	0	Aprovada
1043	498	69	2019-01-24 14:07:36-02	0.0201	174936.56	237688.64	62752.08	3895.89	117	6	Aprovada
1630	267	69	2011-08-14 15:34:21-03	0.0103	126546.13	227988.54	101442.41	5572.80	26	5	Aprovada
1647	555	69	2021-02-25 22:43:57-03	0.0142	132888.48	171688.88	38800.40	4844.51	35	1	Aprovada
1933	300	69	2011-04-23 12:57:18-03	0.0152	49759.58	82370.13	32610.55	1374.02	53	0	Aprovada
430	416	70	2019-05-23 17:17:46-03	0.0210	53057.33	101896.27	48838.94	9504.09	6	2	Aprovada
654	691	70	2012-04-17 23:12:19-03	0.0232	55878.03	95783.77	39905.74	2067.54	43	5	Aprovada
1031	72	70	2021-11-20 15:36:28-03	0.0185	108182.03	193274.48	85092.45	3118.59	56	5	Aprovada
1207	204	70	2016-11-10 23:06:43-02	0.0081	6289.36	7499.18	1209.82	153.48	50	6	Aprovada
1927	622	70	2016-06-06 13:49:17-03	0.0179	78293.07	132986.60	54693.53	2512.20	46	5	Aprovada
851	537	71	2017-05-07 17:54:56-03	0.0083	17825.90	24164.20	6338.30	303.16	81	2	Aprovada
1112	50	71	2019-01-17 23:35:51-02	0.0104	84144.26	159937.79	75793.53	1750.11	67	6	Aprovada
1257	55	71	2020-10-09 01:39:45-03	0.0227	159066.08	244202.87	85136.79	5753.87	44	4	Aprovada
1280	341	71	2015-09-11 06:48:08-03	0.0098	41313.92	57927.35	16613.43	600.52	115	0	Aprovada
1331	185	71	2011-08-15 11:10:06-03	0.0190	109200.42	137396.11	28195.69	2332.73	117	5	Aprovada
314	796	72	2010-02-22 05:48:59-03	0.0142	123811.57	163151.10	39339.53	3524.16	49	3	Aprovada
631	450	72	2012-02-19 00:23:20-02	0.0092	120330.91	213323.10	92992.19	15670.73	8	1	Aprovada
996	825	72	2010-03-28 13:56:06-03	0.0164	50949.47	101453.03	50503.56	1269.42	66	4	Aprovada
1136	940	72	2022-07-24 22:31:49-03	0.0243	49883.45	91065.36	41181.91	2132.48	35	2	Aprovada
1453	273	72	2013-04-24 10:11:23-03	0.0094	31657.40	52661.87	21004.47	502.08	96	4	Aprovada
322	133	73	2013-02-25 18:58:19-03	0.0247	59772.31	105722.56	45950.25	1562.02	119	3	Aprovada
591	8	73	2022-12-07 19:35:16-03	0.0151	13739.00	19634.77	5895.77	293.27	82	4	Aprovada
628	478	73	2012-06-02 01:15:10-03	0.0207	94742.65	134684.58	39941.93	2960.70	53	5	Aprovada
1416	438	73	2021-01-22 23:40:11-03	0.0162	36243.95	47235.20	10991.25	2049.90	21	3	Aprovada
1659	408	73	2013-05-14 18:17:20-03	0.0086	142604.91	189689.52	47084.61	2587.95	75	4	Aprovada
1675	729	73	2021-07-19 08:14:53-03	0.0226	154071.59	208040.39	53968.80	9296.03	21	1	Aprovada
1731	728	73	2011-11-15 18:46:04-02	0.0113	112833.11	136361.06	23527.95	2150.13	80	5	Aprovada
1788	641	73	2012-12-25 13:05:44-02	0.0226	124829.07	159956.42	35127.35	8517.91	18	2	Aprovada
290	560	74	2013-03-07 19:13:26-03	0.0205	69137.77	103454.61	34316.84	1587.67	110	1	Aprovada
1493	671	74	2022-05-01 02:41:11-03	0.0111	161797.49	199091.85	37294.36	12542.08	14	2	Aprovada
792	813	75	2018-03-06 20:54:00-03	0.0211	9976.34	16642.17	6665.83	236.96	105	5	Aprovada
801	299	75	2012-11-02 11:16:45-02	0.0218	173842.06	214377.73	40535.67	4218.69	106	5	Aprovada
1230	4	75	2011-11-25 16:35:38-02	0.0100	118180.54	223198.55	105018.01	2428.77	67	4	Aprovada
1756	735	75	2022-11-29 07:16:17-03	0.0154	63844.52	105904.92	42060.40	1865.35	49	2	Aprovada
1957	101	75	2015-02-10 11:50:10-02	0.0188	175583.47	211606.04	36022.57	3833.25	106	0	Aprovada
145	264	76	2021-12-06 18:08:15-03	0.0217	3769.86	5204.11	1434.25	90.53	109	5	Aprovada
740	368	76	2016-09-08 13:32:09-03	0.0246	13571.68	22966.63	9394.95	371.72	94	6	Aprovada
744	983	76	2012-09-14 22:11:19-03	0.0222	164685.50	247680.40	82994.90	4954.00	61	3	Aprovada
1152	772	76	2014-09-01 20:38:14-03	0.0219	35977.97	60449.42	24471.45	936.43	85	5	Aprovada
1740	106	76	2013-07-01 23:14:50-03	0.0158	104385.14	127554.48	23169.34	2854.57	55	6	Aprovada
141	369	77	2010-09-20 16:52:34-03	0.0134	31005.55	47339.86	16334.31	542.65	109	0	Aprovada
260	472	77	2022-11-15 07:47:20-03	0.0191	141927.15	226944.41	85017.26	4108.08	57	2	Aprovada
1421	679	77	2012-08-16 22:47:56-03	0.0116	14995.90	22395.26	7399.36	240.93	111	5	Aprovada
1559	981	77	2016-04-15 22:51:59-03	0.0188	181322.59	224026.85	42704.26	5020.80	61	4	Aprovada
1102	991	78	2015-08-25 02:10:22-03	0.0227	58849.50	96878.29	38028.79	1479.17	104	5	Aprovada
1128	331	78	2015-04-13 15:35:01-03	0.0150	56316.20	69678.10	13361.90	1020.94	118	1	Aprovada
1212	750	78	2011-03-14 17:48:05-03	0.0132	37688.30	47988.14	10299.84	1196.21	41	1	Aprovada
1284	633	78	2018-03-13 03:32:52-03	0.0138	102372.91	172118.26	69745.35	1774.71	116	3	Aprovada
1821	940	78	2013-01-06 20:41:12-02	0.0246	85883.00	136214.95	50331.95	4081.43	30	5	Aprovada
123	140	79	2020-10-08 18:02:16-03	0.0220	93266.08	142590.32	49324.24	4890.03	25	4	Aprovada
435	438	79	2011-05-22 03:43:48-03	0.0201	106098.41	179395.89	73297.48	7820.35	16	0	Aprovada
870	465	79	2014-06-25 23:23:59-03	0.0126	82357.95	98115.98	15758.03	2139.52	53	0	Aprovada
696	4	80	2012-12-20 22:22:05-02	0.0187	195223.64	248227.20	53003.56	12302.92	19	1	Aprovada
699	513	80	2018-03-20 14:46:19-03	0.0168	79986.56	103574.82	23588.26	2627.17	43	0	Aprovada
921	290	80	2011-01-02 12:15:36-02	0.0195	87947.18	103893.97	15946.79	2107.73	87	0	Aprovada
1519	557	80	2015-11-12 02:30:36-02	0.0250	125084.26	164040.07	38955.81	3316.20	116	2	Aprovada
1700	775	80	2019-05-07 02:07:29-03	0.0116	128716.75	229765.57	101048.82	2720.80	69	2	Aprovada
1763	259	80	2018-11-28 22:28:49-02	0.0203	104327.98	165881.46	61553.48	10678.22	11	5	Aprovada
6	661	81	2018-08-02 21:21:34-03	0.0130	103186.42	166482.48	63296.06	1734.04	115	1	Aprovada
347	439	81	2011-03-14 16:06:37-03	0.0239	74098.83	115129.09	41030.26	7740.22	11	0	Aprovada
954	58	81	2022-10-31 00:38:13-03	0.0114	115558.30	170547.26	54988.96	6798.93	19	1	Aprovada
1160	67	81	2012-09-15 03:41:15-03	0.0098	12906.94	15601.18	2694.24	308.96	54	5	Aprovada
2	179	82	2010-09-09 10:28:06-03	0.0187	57299.89	88665.75	31365.86	1218.98	114	3	Aprovada
331	936	82	2015-01-30 02:30:47-02	0.0193	147816.64	192062.19	44245.55	150669.50	1	1	Aprovada
813	369	82	2018-08-20 08:55:08-03	0.0186	92902.91	150229.48	57326.57	3635.22	35	6	Aprovada
998	306	82	2014-05-22 03:34:07-03	0.0249	83746.16	110506.70	26760.54	2405.39	82	6	Aprovada
1030	197	82	2020-09-09 03:29:31-03	0.0124	155074.39	229773.46	74699.07	5142.39	38	1	Aprovada
1096	418	82	2010-05-20 16:44:32-03	0.0161	9723.63	18409.48	8685.85	382.14	33	1	Aprovada
1243	565	82	2021-08-27 00:09:36-03	0.0183	110475.29	190705.43	80230.14	2513.04	90	2	Aprovada
1541	451	82	2022-07-30 08:38:15-03	0.0081	102125.02	133649.41	31524.39	1534.57	96	2	Aprovada
1577	673	82	2021-03-30 22:29:30-03	0.0101	11791.17	18333.52	6542.35	173.82	115	0	Aprovada
1906	350	82	2015-10-11 22:55:08-03	0.0158	5461.50	10688.21	5226.71	102.70	117	3	Aprovada
856	503	83	2015-11-08 13:19:00-02	0.0147	176083.67	220829.65	44745.98	4532.83	58	1	Aprovada
919	860	83	2015-02-07 10:47:27-02	0.0177	135570.20	186453.35	50883.15	12639.07	12	1	Aprovada
1300	759	83	2012-04-30 13:41:59-03	0.0168	10527.89	15865.16	5337.27	246.30	76	1	Aprovada
1373	583	83	2014-09-12 06:47:44-03	0.0112	9304.64	11085.34	1780.70	832.99	12	4	Aprovada
48	920	84	2018-11-17 04:58:44-02	0.0210	31644.82	56782.82	25138.00	878.28	68	2	Aprovada
481	371	84	2010-06-11 12:33:25-03	0.0228	186512.32	223778.68	37266.36	5114.07	79	6	Aprovada
554	191	84	2019-08-19 02:44:20-03	0.0150	106500.04	140843.81	34343.77	4826.08	27	0	Aprovada
1172	35	84	2012-05-10 10:55:07-03	0.0090	57051.68	85287.56	28235.88	28911.51	2	6	Aprovada
1405	856	84	2011-09-05 01:50:26-03	0.0188	64049.71	85282.05	21232.34	2064.34	47	5	Aprovada
1497	165	84	2014-11-29 18:29:45-02	0.0227	64909.02	109554.72	44645.70	1859.90	70	4	Aprovada
1868	425	84	2022-03-15 20:33:58-03	0.0233	109606.22	168493.12	58886.90	2743.48	116	1	Aprovada
134	909	85	2016-09-30 20:37:19-03	0.0124	147535.76	209417.93	61882.17	12358.26	13	3	Aprovada
218	191	85	2011-07-18 03:14:37-03	0.0201	183838.27	224931.56	41093.29	39014.15	5	4	Aprovada
309	564	85	2020-01-21 16:43:11-03	0.0135	32649.57	56293.75	23644.18	552.87	119	1	Aprovada
647	938	85	2020-10-05 08:48:51-03	0.0081	115525.22	195909.91	80384.69	6280.08	20	0	Aprovada
674	904	85	2021-06-09 12:16:45-03	0.0141	20523.90	31494.17	10970.27	389.56	97	2	Aprovada
1301	5	85	2014-05-22 06:44:17-03	0.0196	152921.38	208821.59	55900.21	10664.14	17	1	Aprovada
1313	316	85	2020-03-11 17:14:18-03	0.0131	116559.93	165625.46	49065.53	1998.15	111	2	Aprovada
73	11	86	2013-08-01 08:26:43-03	0.0164	116948.61	159991.26	43042.65	8369.59	16	3	Aprovada
83	360	86	2014-06-05 06:52:56-03	0.0195	21361.55	29974.42	8612.87	664.85	51	1	Aprovada
439	998	86	2016-09-17 00:47:03-03	0.0185	18134.25	23060.59	4926.34	457.80	72	6	Aprovada
636	372	86	2019-06-11 04:40:10-03	0.0163	116099.08	177649.13	61550.05	20468.63	6	6	Aprovada
716	597	86	2010-06-14 04:15:45-03	0.0191	146368.57	214521.63	68153.06	3281.06	101	3	Aprovada
923	38	86	2020-07-30 12:26:47-03	0.0212	186247.64	229957.55	43709.91	6644.18	43	6	Aprovada
20	295	87	2011-08-03 10:42:31-03	0.0135	184155.00	233173.98	49018.98	3129.10	118	0	Aprovada
282	168	87	2013-12-03 03:41:13-02	0.0225	5068.72	7684.05	2615.33	2620.21	2	6	Aprovada
809	225	87	2016-11-22 16:03:15-02	0.0147	11154.79	18381.85	7227.06	233.53	83	4	Aprovada
834	132	87	2013-11-09 06:32:22-02	0.0117	84568.88	122383.67	37814.79	85558.34	1	1	Aprovada
876	396	87	2011-12-25 18:27:50-02	0.0217	14469.35	22074.86	7605.51	14783.33	1	1	Aprovada
1474	123	87	2019-06-27 21:53:04-03	0.0126	104405.01	162018.64	57613.63	8181.34	14	0	Aprovada
1673	28	87	2018-05-30 13:04:13-03	0.0132	139839.83	240449.39	100609.56	2861.30	79	5	Aprovada
1687	97	87	2014-04-08 07:59:32-03	0.0238	8478.48	15061.37	6582.89	266.86	60	0	Aprovada
1949	928	87	2019-04-12 16:01:14-03	0.0224	33744.09	61689.62	27945.53	2111.78	20	2	Aprovada
105	948	88	2016-08-07 22:19:26-03	0.0212	5054.66	9340.30	4285.64	125.35	92	2	Aprovada
536	595	88	2016-01-20 20:09:25-02	0.0168	54151.64	105985.92	51834.28	2255.32	31	4	Aprovada
1345	872	88	2017-07-19 18:17:48-03	0.0174	110632.42	181574.05	70941.63	3862.13	40	4	Aprovada
1865	234	88	2014-11-19 03:30:19-02	0.0119	128592.68	248405.95	119813.27	3119.88	57	4	Aprovada
223	909	89	2017-02-09 06:54:50-02	0.0171	122502.91	194990.46	72487.55	2435.53	116	4	Aprovada
340	425	89	2010-09-09 19:49:32-03	0.0177	83015.70	165487.90	82472.20	1699.32	114	5	Aprovada
1073	979	89	2016-08-07 05:21:22-03	0.0099	185333.34	226866.22	41532.88	4791.93	49	4	Aprovada
1178	664	89	2022-10-02 03:14:43-03	0.0205	41309.92	60180.57	18870.65	1039.82	83	4	Aprovada
1289	727	89	2015-08-19 05:01:47-03	0.0097	112400.39	143072.18	30671.79	2116.28	75	4	Aprovada
984	492	90	2016-07-23 11:36:42-03	0.0249	13082.90	21427.62	8344.72	435.66	56	2	Aprovada
1267	682	90	2014-12-31 04:07:12-02	0.0197	92780.48	134662.28	41881.80	2563.22	64	1	Aprovada
1382	549	90	2010-04-21 15:44:21-03	0.0213	93389.80	120605.60	27215.80	2169.63	118	1	Aprovada
1642	702	90	2012-09-16 09:09:03-03	0.0138	67181.27	125046.97	57865.70	1242.70	100	6	Aprovada
396	216	91	2011-06-10 21:58:32-03	0.0216	15669.85	23617.02	7947.17	715.14	30	5	Aprovada
1127	518	91	2020-05-03 04:29:37-03	0.0148	53583.39	78606.22	25022.83	18392.41	3	0	Aprovada
1672	849	91	2013-03-15 06:17:38-03	0.0245	75163.87	116332.44	41168.57	2103.94	86	6	Aprovada
855	271	92	2012-01-20 11:16:55-02	0.0233	150893.60	233031.73	82138.13	4624.75	62	2	Aprovada
1418	81	92	2012-06-10 19:30:10-03	0.0091	190412.93	248007.51	57594.58	96508.00	2	1	Aprovada
1481	303	92	2015-01-16 06:00:35-02	0.0186	55585.78	73368.85	17783.07	1294.35	87	6	Aprovada
907	235	93	2013-12-22 05:15:09-02	0.0105	33146.90	44139.93	10993.03	522.55	105	6	Aprovada
1070	527	93	2022-04-13 20:49:59-03	0.0210	163470.99	217503.14	54032.15	3986.42	95	0	Aprovada
1213	637	93	2010-08-02 22:03:08-03	0.0178	139806.77	231570.63	91763.86	8737.22	19	1	Aprovada
188	635	94	2016-10-08 16:08:34-03	0.0228	13356.92	19503.93	6147.01	1005.69	16	3	Aprovada
506	742	94	2015-11-14 05:28:00-02	0.0205	49202.42	68158.82	18956.40	1141.48	106	0	Aprovada
1026	181	94	2018-07-21 17:24:01-03	0.0126	137778.80	233453.55	95674.75	12443.48	12	3	Aprovada
1064	831	94	2012-09-01 11:17:08-03	0.0237	112233.01	175543.59	63310.58	3706.05	54	5	Aprovada
1527	951	94	2013-02-06 04:14:55-02	0.0114	29919.04	59601.77	29682.73	641.01	67	6	Aprovada
1657	143	94	2012-12-04 14:03:16-02	0.0234	59345.81	88251.41	28905.60	3609.27	21	2	Aprovada
1913	699	94	2016-01-16 02:12:58-02	0.0210	33977.58	67827.46	33849.88	1207.65	43	2	Aprovada
339	214	96	2014-08-06 02:00:39-03	0.0157	20300.21	33173.70	12873.49	6980.31	3	0	Aprovada
465	636	96	2018-03-04 04:00:16-03	0.0171	43189.58	78054.70	34865.12	1326.27	48	5	Aprovada
602	192	96	2021-12-29 13:03:05-03	0.0240	48985.85	97783.57	48797.72	1266.73	111	4	Aprovada
1747	742	96	2021-12-28 22:19:30-03	0.0148	23857.95	35765.89	11907.94	602.72	60	1	Aprovada
351	36	97	2012-10-13 04:03:12-03	0.0153	164684.02	228215.07	63531.05	3052.07	115	6	Aprovada
511	782	97	2022-09-18 11:47:48-03	0.0216	126307.61	243616.60	117308.99	4539.15	43	5	Aprovada
903	845	97	2019-04-27 15:05:07-03	0.0239	95121.37	167885.94	72764.57	5424.05	23	3	Aprovada
1130	15	97	2022-07-24 18:05:07-03	0.0110	30531.58	41233.61	10702.03	489.29	106	3	Aprovada
1211	802	97	2018-09-13 20:33:09-03	0.0238	14199.55	24425.14	10225.59	393.86	83	3	Aprovada
1445	832	97	2021-01-04 22:58:46-03	0.0110	117153.43	148156.89	31003.46	4480.58	31	6	Aprovada
1463	193	97	2011-11-10 21:13:01-02	0.0116	8431.56	16504.27	8072.71	135.46	111	6	Aprovada
1810	191	97	2015-10-01 10:51:28-03	0.0086	60684.48	93439.38	32754.90	1071.13	78	6	Aprovada
1948	156	97	2011-06-29 11:18:57-03	0.0093	14963.27	18834.47	3871.20	237.89	95	4	Aprovada
1312	813	98	2021-10-05 04:36:19-03	0.0174	13271.07	22344.63	9073.56	292.93	90	4	Aprovada
1334	672	98	2014-06-14 09:16:36-03	0.0117	158040.06	198474.63	40434.57	9325.03	19	3	Aprovada
1436	821	98	2013-08-16 09:38:48-03	0.0235	102961.22	135519.05	32557.83	2613.39	112	4	Aprovada
1696	75	98	2011-06-17 01:03:46-03	0.0248	9723.65	12962.70	3239.05	328.70	54	4	Aprovada
1929	978	98	2012-11-07 22:22:02-02	0.0157	48415.59	77644.96	29229.37	1003.19	91	4	Aprovada
26	83	99	2011-01-27 06:53:45-02	0.0093	190280.36	234441.83	44161.47	3108.28	91	6	Aprovada
363	49	99	2015-02-01 14:20:11-02	0.0112	125315.82	217409.12	92093.30	2544.80	72	5	Aprovada
759	907	99	2012-10-26 21:37:44-02	0.0131	84750.42	159885.48	75135.06	1728.41	79	2	Aprovada
993	46	99	2020-11-03 00:51:22-03	0.0166	7990.37	11443.64	3453.27	182.29	79	6	Aprovada
1253	899	99	2014-02-04 01:34:28-02	0.0103	153132.16	187128.35	33996.19	3995.57	49	2	Aprovada
1356	606	99	2013-05-19 15:03:06-03	0.0203	122510.45	202342.29	79831.84	5779.12	28	4	Aprovada
1378	353	99	2018-06-12 00:57:45-03	0.0231	63497.22	102910.38	39413.16	1755.82	79	0	Aprovada
1528	593	99	2016-08-12 00:09:18-03	0.0123	137478.28	241807.06	104328.78	6210.28	26	6	Aprovada
1836	348	99	2014-07-13 03:30:03-03	0.0189	2137.03	3534.67	1397.64	73.04	43	6	Aprovada
126	881	100	2015-05-19 12:45:09-03	0.0207	64165.87	102698.22	38532.35	1596.85	87	3	Aprovada
1899	963	100	2015-01-31 01:13:39-02	0.0149	52742.84	68446.29	15703.45	1000.82	104	0	Aprovada
397	679	1	2016-03-09 06:28:40-03	0.0226	61955.25	87290.12	25334.87	1524.99	112	1	Em análise
768	144	1	2016-01-28 01:50:17-02	0.0246	77445.59	119795.73	42350.14	2066.21	105	3	Em análise
888	690	1	2019-12-01 08:00:58-03	0.0094	79516.51	140657.41	61140.90	2676.65	35	0	Em análise
915	272	1	2017-07-02 14:05:59-03	0.0250	107635.43	158620.45	50985.02	7185.81	19	1	Em análise
1017	843	1	2010-01-03 18:05:18-02	0.0228	76028.56	138815.88	62787.32	2024.77	86	6	Em análise
1670	370	1	2016-07-20 09:27:44-03	0.0154	96552.03	133711.72	37159.69	1960.07	93	5	Em análise
1236	920	2	2021-02-17 20:51:34-03	0.0137	23959.92	42326.16	18366.24	443.58	99	1	Em análise
1620	540	2	2012-06-08 05:03:18-03	0.0236	21384.38	25395.30	4010.92	11072.17	2	0	Em análise
1729	647	2	2010-09-23 13:39:54-03	0.0142	88228.67	126716.15	38487.48	1550.75	117	2	Em análise
317	331	3	2011-07-20 16:19:38-03	0.0175	59473.07	84424.06	24950.99	2155.89	38	3	Em análise
311	166	4	2014-03-22 06:09:55-03	0.0220	119945.29	143228.38	23283.09	4172.02	46	0	Em análise
830	223	4	2022-02-24 05:54:54-03	0.0168	49903.52	70159.91	20256.39	1152.67	78	3	Em análise
1484	31	4	2021-07-13 10:37:16-03	0.0117	137722.27	207913.53	70191.26	2840.82	72	5	Em análise
1645	169	4	2021-01-12 20:53:55-03	0.0081	23609.29	31956.29	8347.00	2252.01	11	2	Em análise
1076	442	5	2011-06-19 00:27:20-03	0.0102	67935.33	85140.20	17204.87	4372.97	17	6	Em análise
1937	251	5	2020-05-04 12:43:28-03	0.0113	127390.90	238393.65	111002.75	2595.06	72	5	Em análise
493	921	6	2022-05-11 22:37:28-03	0.0136	71727.56	113574.72	41847.16	14936.08	5	2	Em análise
671	592	6	2011-04-26 22:58:33-03	0.0174	156914.78	207172.35	50257.57	8645.66	22	2	Em análise
1622	539	6	2021-02-06 06:13:35-03	0.0181	1636.69	1952.81	316.12	37.15	89	1	Em análise
387	555	7	2012-07-05 09:54:05-03	0.0093	141256.48	166243.96	24987.48	1958.62	120	5	Em análise
595	469	7	2018-06-16 01:28:43-03	0.0083	96236.52	139447.52	43211.00	1636.65	81	5	Em análise
939	213	7	2015-05-28 21:59:18-03	0.0165	14734.15	25810.81	11076.66	377.88	63	4	Em análise
1433	495	7	2013-08-12 01:15:35-03	0.0090	129773.93	230460.81	100686.88	2208.42	84	1	Em análise
1991	599	7	2018-06-20 09:59:34-03	0.0250	12543.90	19414.09	6870.19	345.05	97	4	Em análise
624	69	8	2018-01-06 05:51:33-02	0.0224	79888.67	109159.52	29270.85	2102.34	86	4	Em análise
1139	753	8	2018-06-11 19:36:09-03	0.0173	64453.97	80451.88	15997.91	1349.71	102	1	Em análise
1678	598	8	2013-08-26 19:45:30-03	0.0228	71043.04	136473.69	65430.65	1914.51	83	5	Em análise
1795	418	8	2018-06-13 12:27:06-03	0.0125	86193.08	119993.16	33800.08	2699.62	41	5	Em análise
201	729	9	2010-03-02 04:15:51-03	0.0134	12337.35	18284.34	5946.99	254.10	79	1	Em análise
235	822	9	2015-09-07 10:21:59-03	0.0249	84777.07	126949.86	42172.79	3533.07	37	5	Em análise
342	762	9	2022-01-29 10:36:45-03	0.0195	78781.21	155280.70	76499.49	2101.40	68	4	Em análise
1233	194	9	2018-04-09 01:18:29-03	0.0220	200859.74	241023.80	40164.06	7171.81	44	2	Em análise
1315	349	9	2013-07-10 22:46:06-03	0.0225	14077.40	18349.97	4272.57	343.31	115	1	Em análise
1951	48	9	2015-09-09 19:13:42-03	0.0187	119479.89	168946.42	49466.53	2541.78	114	4	Em análise
486	482	10	2022-07-18 17:48:45-03	0.0126	125288.10	151666.94	26378.84	2085.24	113	3	Em análise
980	703	10	2010-10-01 08:10:59-03	0.0126	104619.55	125779.06	21159.51	1695.58	120	0	Em análise
1161	466	10	2012-02-25 08:38:52-02	0.0243	135995.57	202864.43	66868.86	3904.88	78	3	Em análise
1305	969	10	2021-04-17 19:53:35-03	0.0154	57348.29	109760.41	52412.12	3672.29	18	6	Em análise
1357	173	10	2011-07-19 10:36:58-03	0.0097	46434.67	80919.20	34484.53	1901.75	28	3	Em análise
1494	162	10	2019-04-08 23:26:52-03	0.0186	168631.68	212208.65	43576.97	8001.32	27	2	Em análise
1596	118	10	2011-09-24 08:16:44-03	0.0130	127549.13	159760.66	32211.53	2738.76	72	1	Em análise
1025	847	11	2017-08-22 10:05:40-03	0.0223	59509.83	91298.96	31789.13	2108.66	45	6	Em análise
1107	980	11	2022-11-18 14:45:26-03	0.0159	19349.66	36140.87	16791.21	412.13	87	6	Em análise
1428	924	11	2012-05-28 05:59:28-03	0.0245	24740.02	39246.81	14506.79	681.44	91	0	Em análise
1451	365	11	2017-03-18 16:49:18-03	0.0166	131772.22	184433.19	52660.97	3301.06	66	4	Em análise
1534	708	11	2018-05-05 19:53:38-03	0.0213	37921.27	51609.70	13688.43	1861.36	27	0	Em análise
1610	821	11	2016-06-07 13:20:11-03	0.0216	81996.21	154433.47	72437.26	7301.79	13	5	Em análise
1904	593	11	2019-02-02 07:43:10-02	0.0082	59032.85	81907.29	22874.44	807.66	112	6	Em análise
45	298	12	2021-11-24 07:34:29-03	0.0249	90801.17	167563.10	76761.93	2431.68	108	0	Em análise
518	133	12	2010-01-20 05:48:50-02	0.0182	165817.19	219868.31	54051.12	5079.15	50	1	Em análise
1197	282	12	2018-08-19 03:28:17-03	0.0086	32204.60	40053.55	7848.95	461.61	107	6	Em análise
1218	948	12	2018-02-11 15:39:17-02	0.0236	100896.21	142068.35	41172.14	2569.64	112	5	Em análise
1232	235	12	2013-01-22 06:20:15-02	0.0188	105870.62	183510.02	77639.40	18824.17	6	2	Em análise
1319	960	12	2015-06-04 11:54:22-03	0.0193	31979.26	38086.40	6107.14	5695.64	6	4	Em análise
1719	276	12	2018-07-29 01:11:23-03	0.0112	119323.20	189724.55	70401.35	3717.42	40	1	Em análise
57	950	13	2013-03-28 20:21:05-03	0.0113	107840.65	144234.53	36393.88	2039.35	81	2	Em análise
202	813	13	2022-09-16 19:30:47-03	0.0177	102638.11	198722.53	96084.42	3377.34	44	5	Em análise
328	835	13	2015-01-06 07:04:26-02	0.0146	31142.43	61877.24	30734.81	602.33	97	1	Em análise
857	182	13	2022-08-24 17:10:51-03	0.0151	131839.59	234725.24	102885.65	2385.75	120	1	Em análise
892	493	13	2011-07-26 20:37:58-03	0.0191	61159.15	105877.22	44718.07	1522.95	77	4	Em análise
908	167	13	2015-12-22 06:10:11-02	0.0211	36978.99	51063.39	14084.40	1101.63	59	1	Em análise
1368	972	13	2010-08-04 06:34:38-03	0.0170	165372.26	209244.59	43872.33	3601.21	90	5	Em análise
97	552	14	2017-10-16 13:04:51-02	0.0201	82610.01	124376.52	41766.51	2000.88	89	0	Em análise
139	722	14	2011-12-17 04:20:56-02	0.0207	166568.54	214209.91	47641.37	4218.12	83	2	Em análise
320	832	14	2012-08-14 01:04:28-03	0.0230	83896.07	130573.16	46677.09	2324.01	78	5	Em análise
831	930	14	2018-01-17 19:12:44-02	0.0082	39450.68	47487.52	8036.84	626.25	89	0	Em análise
1068	219	14	2015-02-04 05:48:16-02	0.0204	40254.62	71168.65	30914.03	1725.26	32	4	Em análise
1425	682	14	2017-12-16 22:23:37-02	0.0199	41794.26	76403.18	34608.92	1669.24	35	4	Em análise
1486	48	14	2015-10-10 05:34:37-03	0.0226	77772.97	113222.40	35449.43	79530.64	1	0	Em análise
1988	7	14	2022-04-05 07:57:41-03	0.0244	82768.58	115375.51	32606.93	4597.20	24	4	Em análise
365	645	15	2013-09-16 13:00:04-03	0.0168	159566.15	208865.39	49299.24	3359.36	96	3	Em análise
600	521	15	2014-01-10 22:16:06-02	0.0107	90076.22	146615.74	56539.52	1768.27	74	3	Em análise
961	123	15	2016-12-31 06:17:11-02	0.0171	152376.97	245028.53	92651.56	3620.80	75	4	Em análise
1061	706	15	2019-04-05 05:25:36-03	0.0136	103239.02	162632.47	59393.45	2276.50	71	2	Em análise
1279	408	15	2011-12-16 17:08:15-02	0.0218	68276.90	81102.92	12826.02	2255.81	50	0	Em análise
1201	726	16	2011-01-24 17:42:38-02	0.0186	90739.47	108373.61	17634.14	2253.43	75	6	Em análise
1617	27	16	2016-03-13 20:48:07-03	0.0095	25773.70	46882.51	21108.81	380.67	109	6	Em análise
1767	400	16	2021-04-13 17:39:24-03	0.0123	56569.25	80760.58	24191.33	1338.66	60	3	Em análise
1839	504	16	2021-05-30 03:14:25-03	0.0155	98488.92	145228.75	46739.83	3154.97	43	3	Em análise
181	667	17	2016-04-07 14:34:04-03	0.0114	61740.46	82757.33	21016.87	1517.19	55	0	Em análise
189	690	17	2020-11-26 21:26:31-03	0.0129	19572.89	32461.13	12888.24	732.05	33	1	Em análise
349	147	17	2012-01-05 06:59:53-02	0.0189	27524.24	52447.68	24923.44	717.27	69	2	Em análise
457	474	17	2020-12-22 23:44:41-03	0.0084	187030.77	238235.74	51204.97	12541.50	16	3	Em análise
1286	799	17	2011-05-14 01:54:51-03	0.0185	190959.72	231287.71	40327.99	17891.14	12	5	Em análise
1618	463	17	2016-01-02 18:36:12-02	0.0211	91289.02	108764.32	17475.30	2233.45	95	2	Em análise
833	324	18	2021-08-31 21:48:21-03	0.0221	130988.74	162328.81	31340.07	3168.78	112	0	Em análise
1144	235	18	2010-07-14 16:20:51-03	0.0118	108675.98	210629.13	101953.15	2662.96	56	4	Em análise
1476	928	18	2018-12-06 12:43:05-02	0.0211	154585.07	236248.08	81663.01	13720.74	13	0	Em análise
1522	273	18	2010-05-25 04:21:37-03	0.0182	125651.84	236633.76	110981.92	4305.29	42	5	Em análise
1591	260	18	2010-09-25 17:06:42-03	0.0193	115049.79	199024.51	83974.72	24359.21	5	4	Em análise
1824	708	18	2016-04-06 12:53:27-03	0.0226	138655.68	197860.73	59205.05	3763.27	80	5	Em análise
737	157	19	2010-10-30 15:18:23-02	0.0217	99389.95	159316.57	59926.62	2365.91	113	6	Em análise
1326	904	19	2016-01-01 01:17:54-02	0.0231	25485.40	37107.30	11621.90	771.80	63	2	Em análise
1523	247	19	2020-07-31 05:21:22-03	0.0215	75466.07	142225.29	66759.22	2536.03	48	3	Em análise
174	530	20	2011-06-10 23:50:28-03	0.0181	51830.18	93711.89	41881.71	4844.03	12	0	Em análise
233	866	20	2020-11-26 19:35:25-03	0.0248	33518.43	65218.60	31700.17	4670.74	8	1	Em análise
305	482	20	2016-09-12 20:53:15-03	0.0198	135352.84	200233.71	64880.87	3440.18	77	0	Em análise
702	910	20	2014-10-16 12:43:01-03	0.0140	98342.11	134155.29	35813.18	3932.17	31	5	Em análise
1379	101	20	2020-07-27 21:07:36-03	0.0193	53602.97	89103.04	35500.07	1722.76	48	4	Em análise
1850	602	20	2012-10-14 18:55:11-03	0.0158	11526.10	22325.93	10799.83	223.16	108	4	Em análise
1863	492	20	2013-10-22 10:19:57-02	0.0151	127406.36	196109.76	68703.40	5780.91	27	4	Em análise
577	752	21	2019-03-31 14:05:35-03	0.0172	16502.49	28916.44	12413.95	379.08	81	6	Em análise
604	911	21	2017-04-05 14:07:50-03	0.0095	111488.42	156289.02	44800.60	2542.12	57	0	Em análise
1666	544	21	2013-03-01 21:43:51-03	0.0239	64049.30	106188.84	42139.54	2124.06	54	2	Em análise
11	706	23	2015-04-12 01:33:31-03	0.0178	7450.56	11583.22	4132.66	152.29	116	1	Em análise
55	607	23	2020-06-04 13:50:24-03	0.0169	82374.84	100418.72	18043.88	8269.17	11	2	Em análise
176	599	23	2010-02-18 16:49:39-02	0.0200	123385.95	217428.35	94042.40	16843.39	8	3	Em análise
815	120	23	2014-07-07 10:26:25-03	0.0135	197133.32	246001.18	48867.86	5372.54	51	3	Em análise
1730	842	23	2011-09-18 11:17:27-03	0.0096	6452.37	12480.59	6028.22	183.86	43	0	Em análise
1922	252	23	2015-09-25 14:06:36-03	0.0119	41283.32	67822.99	26539.67	697.52	103	6	Em análise
771	557	24	2016-12-15 12:48:48-02	0.0159	8192.68	9644.34	1451.66	173.58	88	0	Em análise
1258	139	24	2014-02-22 09:31:44-03	0.0247	64667.96	79608.76	14940.80	4305.58	19	2	Em análise
434	859	25	2017-06-27 14:45:08-03	0.0096	52629.60	77727.42	25097.82	1104.49	64	3	Em análise
742	83	25	2017-11-10 14:34:56-02	0.0113	144793.34	219831.34	75038.00	3375.78	59	4	Em análise
155	140	26	2010-08-10 11:49:13-03	0.0127	109902.59	205505.77	95603.18	1946.91	100	4	Em análise
183	803	26	2017-05-03 09:20:24-03	0.0084	140983.59	241213.51	100229.92	2089.49	100	1	Em análise
738	120	26	2016-09-04 08:18:44-03	0.0108	24392.71	40416.16	16023.45	1426.95	19	5	Em análise
1058	44	26	2014-03-29 10:57:44-03	0.0080	112878.57	162656.39	49777.82	2942.72	46	1	Em análise
1100	904	26	2017-06-29 06:05:28-03	0.0220	168843.92	244055.69	75211.77	5054.88	61	2	Em análise
1526	735	26	2022-05-29 01:33:30-03	0.0108	110065.12	194494.67	84429.55	8513.42	14	3	Em análise
1648	323	26	2022-01-21 08:07:44-03	0.0199	10436.76	12302.68	1865.92	229.24	120	6	Em análise
1771	61	26	2014-10-14 21:14:58-03	0.0225	27737.84	46672.24	18934.40	757.68	78	0	Em análise
1794	559	26	2019-04-24 07:21:51-03	0.0093	12449.86	17115.37	4665.51	464.13	31	4	Em análise
336	218	27	2022-12-06 21:48:42-03	0.0087	16572.92	27027.59	10454.67	272.38	87	3	Em análise
570	275	27	2014-05-20 12:55:55-03	0.0249	17183.36	24405.58	7222.22	462.85	105	5	Em análise
1318	811	27	2019-03-01 07:32:37-03	0.0217	159992.40	204814.51	44822.11	10365.29	19	3	Em análise
1420	661	27	2012-06-15 06:19:45-03	0.0229	115475.14	175842.38	60367.24	2958.90	99	6	Em análise
1483	905	27	2012-02-13 01:17:30-02	0.0235	130116.96	245412.65	115295.69	5396.17	36	4	Em análise
1491	250	27	2016-10-23 10:31:21-02	0.0106	147577.15	224493.97	76916.82	7555.91	22	3	Em análise
130	959	28	2013-03-29 08:26:37-03	0.0181	31596.57	59950.75	28354.18	754.89	79	0	Em análise
790	233	28	2010-08-12 16:51:10-03	0.0135	40280.10	77997.85	37717.75	1129.03	49	0	Em análise
1141	57	28	2016-12-08 04:51:57-02	0.0190	25852.69	48288.60	22435.91	645.64	76	4	Em análise
1695	497	28	2010-05-14 19:57:17-03	0.0085	22425.95	29841.47	7415.52	349.85	93	0	Em análise
1829	768	28	2017-01-22 00:59:22-02	0.0181	117945.75	182004.77	64059.02	2609.58	95	0	Em análise
901	865	29	2012-02-13 19:49:42-02	0.0131	110241.56	140949.38	30707.82	2105.23	89	3	Em análise
1013	410	29	2015-12-06 15:09:10-02	0.0175	90568.70	173506.78	82938.08	3482.44	35	5	Em análise
1032	250	29	2010-05-23 04:53:02-03	0.0161	76870.65	136453.06	59582.41	2773.67	37	0	Em análise
1065	400	29	2019-07-12 10:08:43-03	0.0180	91507.72	175569.20	84061.48	3786.78	32	6	Em análise
1714	307	29	2013-10-24 06:19:30-02	0.0233	110972.56	195170.00	84197.44	3268.16	68	5	Em análise
1860	70	29	2011-08-19 04:03:50-03	0.0212	70038.21	84072.66	14034.45	2368.39	47	0	Em análise
326	478	30	2020-09-28 23:37:44-03	0.0231	19627.61	25977.11	6349.50	697.28	46	3	Em análise
825	298	30	2010-12-19 06:59:00-02	0.0207	186109.74	231127.83	45018.09	12490.44	18	5	Em análise
859	312	30	2021-08-03 02:47:19-03	0.0085	145554.63	188606.65	43052.02	5517.17	30	0	Em análise
1006	49	30	2013-10-19 23:34:28-03	0.0121	134857.11	170072.67	35215.56	2243.96	108	6	Em análise
1278	525	30	2021-11-10 07:42:39-03	0.0172	132846.65	201870.30	69023.65	2623.96	120	5	Em análise
1335	198	30	2019-02-14 11:34:26-02	0.0158	165204.91	217250.63	52045.72	3125.42	115	5	Em análise
1750	756	30	2020-04-14 08:58:17-03	0.0211	46664.99	71877.71	25212.72	1290.05	69	4	Em análise
1760	855	30	2013-03-05 01:45:03-03	0.0098	103363.73	149314.80	45951.07	3201.83	39	5	Em análise
108	322	31	2015-09-09 18:35:42-03	0.0236	16522.42	25092.26	8569.84	882.47	25	5	Em análise
177	285	31	2019-06-20 16:30:30-03	0.0137	86285.14	112021.34	25736.20	7291.15	13	3	Em análise
721	247	31	2011-03-30 18:44:36-03	0.0144	165121.88	201305.07	36183.19	3791.51	69	3	Em análise
735	698	31	2016-08-25 01:41:10-03	0.0112	22281.63	30661.75	8380.12	350.13	112	6	Em análise
747	403	31	2015-11-17 06:52:56-02	0.0178	36091.21	71462.40	35371.19	730.33	120	2	Em análise
1092	767	31	2012-07-06 20:57:48-03	0.0084	129802.84	196604.34	66801.50	65720.32	2	4	Em análise
1248	935	31	2015-05-13 18:11:25-03	0.0245	106501.49	154995.51	48494.02	3061.68	79	3	Em análise
1332	736	31	2012-05-10 08:32:25-03	0.0245	45176.08	89767.63	44591.55	1379.31	67	6	Em análise
1827	52	31	2013-10-17 08:31:45-03	0.0099	187312.95	230434.09	43121.14	3487.97	77	5	Em análise
84	869	32	2017-12-31 17:42:56-02	0.0173	78125.38	114587.70	36462.32	2476.77	46	5	Em análise
827	246	32	2013-05-18 07:54:23-03	0.0207	25739.07	47781.82	22042.75	675.06	76	4	Em análise
1385	768	32	2021-07-09 15:19:13-03	0.0231	139365.42	212969.31	73603.89	4834.88	48	5	Em análise
1665	490	32	2010-04-14 15:17:56-03	0.0162	109303.53	188133.22	78829.69	3340.16	47	0	Em análise
1880	425	32	2016-07-11 13:55:15-03	0.0105	77502.48	142132.71	64630.23	2432.10	39	1	Em análise
627	228	33	2014-09-06 04:17:33-03	0.0107	97977.05	116474.55	18497.50	1618.79	98	5	Em análise
1259	174	33	2022-12-05 11:25:10-03	0.0237	17247.54	34326.14	17078.60	1137.97	19	6	Em análise
1343	233	33	2019-09-07 23:01:21-03	0.0206	25643.19	44769.69	19126.50	623.83	92	4	Em análise
1403	104	33	2017-12-31 14:48:23-02	0.0083	105541.66	181977.25	76435.59	3178.90	39	6	Em análise
1715	874	33	2016-02-20 15:55:53-02	0.0233	1851.55	2551.30	699.75	47.93	100	0	Em análise
199	592	34	2021-05-26 12:48:16-03	0.0125	6964.23	11002.49	4038.26	158.73	64	5	Em análise
651	111	34	2017-10-07 18:17:43-03	0.0191	145257.32	245895.49	100638.17	7361.77	25	5	Em análise
707	825	34	2010-03-25 13:26:46-03	0.0139	34347.37	52262.39	17915.02	957.67	50	4	Em análise
1097	26	34	2022-06-28 13:07:43-03	0.0214	145855.94	182661.44	36805.50	4270.31	62	6	Em análise
733	982	35	2014-09-13 15:08:10-03	0.0213	149854.83	244315.53	94460.70	3516.87	113	1	Em análise
1406	120	35	2014-03-07 05:04:51-03	0.0149	111251.43	181158.62	69907.19	9476.77	13	3	Em análise
1586	526	35	2013-07-02 23:44:11-03	0.0131	44197.99	72577.21	28379.22	1426.67	40	4	Em análise
47	56	36	2021-12-31 08:29:02-03	0.0125	105484.71	145685.49	40200.78	1983.24	88	3	Em análise
836	588	36	2010-02-25 15:00:41-03	0.0111	27290.08	39950.02	12659.94	923.72	36	1	Em análise
842	917	36	2014-11-24 21:10:04-02	0.0198	85653.03	109797.61	24144.58	5453.03	19	1	Em análise
1397	619	36	2012-08-23 03:51:29-03	0.0132	130275.83	159428.49	29152.66	3058.38	63	2	Em análise
1823	145	36	2019-01-08 05:49:03-02	0.0102	97072.12	139604.99	42532.87	2488.08	50	5	Em análise
1945	191	36	2018-05-19 14:36:28-03	0.0177	11567.16	20613.62	9046.46	576.60	25	1	Em análise
421	986	37	2010-10-03 11:19:41-03	0.0248	105677.23	168762.35	63085.12	28077.36	4	4	Em análise
426	231	37	2015-04-25 00:25:28-03	0.0167	44141.84	62490.81	18348.97	929.99	95	2	Em análise
693	789	37	2010-08-17 13:35:43-03	0.0148	97544.40	169529.40	71985.00	1843.72	104	2	Em análise
211	816	38	2012-03-21 17:41:52-03	0.0165	31314.23	48637.75	17323.52	3772.67	9	1	Em análise
283	898	38	2011-03-15 01:20:52-03	0.0099	96645.43	150680.51	54035.08	1484.42	105	2	Em análise
566	228	38	2012-05-24 12:27:52-03	0.0211	130658.60	218354.83	87696.23	133415.49	1	6	Em análise
777	914	38	2015-03-12 14:02:22-03	0.0185	38545.16	57307.10	18761.94	861.30	96	4	Em análise
977	546	38	2022-09-10 05:23:54-03	0.0121	21209.41	37558.68	16349.27	576.31	49	3	Em análise
1005	751	38	2018-11-07 11:42:08-02	0.0224	123500.97	173284.34	49783.37	3677.18	63	0	Em análise
163	351	39	2015-02-24 14:31:12-03	0.0212	42111.08	64496.04	22384.96	1037.10	94	0	Em análise
438	252	39	2017-04-06 18:14:59-03	0.0188	120199.91	225672.23	105472.32	2933.23	79	0	Em análise
462	647	39	2014-02-01 01:41:21-02	0.0145	112475.00	197900.88	85425.88	4778.52	29	5	Em análise
843	194	39	2022-08-18 21:55:57-03	0.0180	123546.66	152562.03	29015.37	2520.11	120	1	Em análise
1082	918	39	2011-04-22 01:39:48-03	0.0109	78303.61	95991.34	17687.73	2768.49	34	5	Em análise
1103	197	39	2020-11-07 09:35:19-03	0.0142	88369.70	164966.27	76596.57	2671.02	45	6	Em análise
1106	245	39	2015-01-26 00:42:19-02	0.0128	25582.20	34251.82	8669.62	426.16	115	4	Em análise
273	831	40	2011-09-09 08:24:54-03	0.0155	64239.20	108649.53	44410.33	1764.80	54	0	Em análise
607	837	40	2020-05-16 08:24:49-03	0.0248	146669.27	197647.71	50978.44	3981.02	100	2	Em análise
1085	328	40	2010-10-09 15:29:03-03	0.0115	138021.30	166540.49	28519.19	5975.63	27	6	Em análise
1263	680	40	2021-10-08 18:00:11-03	0.0183	120055.61	237277.28	117221.67	3254.19	62	1	Em análise
1634	89	40	2021-05-31 15:45:33-03	0.0096	28144.09	37257.18	9113.09	2023.58	15	4	Em análise
291	417	41	2018-02-09 23:44:53-02	0.0158	87329.69	169641.19	82311.50	2286.60	59	0	Em análise
520	86	41	2018-09-29 08:28:52-03	0.0150	141778.31	201697.94	59919.63	2621.36	112	5	Em análise
670	49	41	2022-02-07 01:09:33-03	0.0155	106553.90	165337.10	58783.20	2379.62	77	6	Em análise
950	805	41	2016-01-25 08:11:55-02	0.0114	65047.89	120998.70	55950.81	1329.24	72	6	Em análise
1509	828	41	2017-11-17 14:49:36-02	0.0107	20351.40	39761.78	19410.38	527.69	50	1	Em análise
1884	604	41	2020-07-21 16:30:23-03	0.0152	3439.26	5144.30	1705.04	88.70	59	0	Em análise
1249	935	42	2017-04-25 10:10:28-03	0.0211	72674.86	129818.38	57143.52	1938.29	75	2	Em análise
1277	235	42	2022-08-21 01:20:52-03	0.0185	43792.49	64373.18	20580.69	937.25	109	5	Em análise
124	491	43	2022-05-06 07:30:29-03	0.0212	40044.79	75589.36	35544.57	4119.68	11	6	Em análise
205	425	43	2020-04-02 05:14:39-03	0.0139	112816.16	197807.13	84990.97	1992.77	112	2	Em análise
256	711	43	2021-08-02 12:02:29-03	0.0182	97061.75	135058.79	37997.04	2005.23	118	5	Em análise
259	10	43	2016-09-01 20:12:46-03	0.0150	94491.59	160159.63	65668.04	19757.18	5	6	Em análise
800	770	43	2010-06-10 01:49:07-03	0.0239	145749.30	184197.69	38448.39	3712.09	118	1	Em análise
828	500	43	2016-11-17 11:31:04-02	0.0203	92781.38	124218.64	31437.26	94664.85	1	0	Em análise
1035	702	43	2015-04-30 11:52:51-03	0.0082	33306.62	59682.83	26376.21	455.69	112	5	Em análise
1275	59	43	2018-04-11 12:20:22-03	0.0148	106179.29	193369.16	87189.87	3747.46	37	5	Em análise
1537	815	43	2016-11-21 04:07:09-02	0.0197	74727.48	91071.93	16344.45	1810.29	86	4	Em análise
255	601	44	2014-12-31 13:09:28-02	0.0209	21103.62	33450.01	12346.39	484.11	117	3	Em análise
299	6	44	2012-07-01 20:19:12-03	0.0159	135008.09	246707.05	111698.96	7612.40	21	3	Em análise
789	278	44	2012-11-13 19:00:43-02	0.0149	70921.47	119311.76	48390.29	1407.11	94	4	Em análise
1247	619	44	2012-12-13 10:13:36-02	0.0219	104992.85	144608.16	39615.31	2556.60	106	5	Em análise
1873	834	44	2011-07-16 05:55:50-03	0.0115	27968.99	42190.42	14221.43	2328.61	13	6	Em análise
93	26	45	2018-08-27 04:56:52-03	0.0169	136405.65	182910.57	46504.92	2690.31	116	4	Em análise
307	749	45	2015-02-04 05:46:29-02	0.0127	71645.48	102797.14	31151.66	4712.35	17	3	Em análise
366	396	45	2021-10-26 13:31:16-03	0.0177	98416.12	139390.84	40974.72	2627.23	62	3	Em análise
521	286	45	2021-11-26 01:09:04-03	0.0249	109917.45	183729.50	73812.05	2943.62	108	5	Em análise
1022	397	45	2011-12-14 09:09:29-02	0.0081	37584.75	44297.93	6713.18	1153.06	38	6	Em análise
1709	621	45	2012-03-10 16:25:50-03	0.0232	68060.22	80872.04	12811.82	2129.24	59	5	Em análise
626	555	46	2012-02-18 07:42:41-02	0.0144	117660.51	194023.58	76363.07	3819.83	41	3	Em análise
656	704	46	2011-05-27 23:50:43-03	0.0090	89337.79	148769.13	59431.34	11624.22	8	4	Em análise
835	61	46	2015-04-20 10:34:13-03	0.0234	53345.22	70172.13	16826.91	1616.02	64	6	Em análise
874	966	46	2010-09-29 09:06:08-03	0.0173	32351.58	47233.90	14882.32	8440.70	4	1	Em análise
1302	867	46	2015-03-01 16:34:29-03	0.0249	34420.33	65887.59	31467.26	1081.06	64	6	Em análise
1352	736	46	2013-06-06 00:36:08-03	0.0127	52260.98	85080.10	32819.12	2737.75	22	0	Em análise
1939	695	46	2020-04-20 21:53:57-03	0.0183	3408.60	6487.10	3078.50	73.53	104	1	Em análise
69	920	47	2011-07-23 15:13:10-03	0.0105	62967.36	109646.59	46679.23	4295.87	16	3	Em análise
89	846	47	2014-04-07 14:12:21-03	0.0177	141143.10	206149.93	65006.83	4644.36	44	4	Em análise
190	751	47	2013-09-03 09:30:04-03	0.0237	44464.31	86507.01	42042.70	8037.43	6	6	Em análise
460	909	47	2021-12-20 04:07:30-03	0.0086	146303.62	178747.63	32444.01	2395.38	87	5	Em análise
694	532	47	2015-09-30 05:08:24-03	0.0100	9563.20	11449.02	1885.82	9658.83	1	3	Em análise
1195	757	47	2020-11-17 11:01:09-03	0.0195	68847.75	125784.77	56937.02	1513.19	113	0	Em análise
1543	172	47	2017-02-15 18:01:29-02	0.0233	97455.12	185670.94	88215.82	2709.97	79	2	Em análise
1806	689	47	2019-07-21 17:43:35-03	0.0190	58201.04	101013.62	42812.58	1531.76	68	4	Em análise
303	77	48	2017-02-21 07:55:58-03	0.0138	32374.58	49216.39	16841.81	1100.52	38	2	Em análise
708	701	48	2010-02-04 22:40:03-02	0.0136	175641.08	232356.67	56715.59	9671.36	21	4	Em análise
787	473	48	2018-09-26 08:31:21-03	0.0175	66561.33	104791.67	38230.34	1526.52	83	4	Em análise
1545	152	48	2011-02-03 20:42:37-02	0.0223	48225.02	87348.45	39123.43	1246.70	90	2	Em análise
1915	500	48	2022-10-02 07:58:19-03	0.0107	157869.45	203521.03	45651.58	2372.04	117	4	Em análise
405	522	49	2019-04-23 22:38:15-03	0.0142	21740.29	34760.37	13020.08	439.40	86	5	Em análise
420	8	49	2014-05-07 11:54:03-03	0.0191	151442.78	249930.50	98487.72	3262.97	115	2	Em análise
525	919	49	2021-08-18 23:39:13-03	0.0221	134799.25	173739.37	38940.12	3395.49	96	6	Em análise
650	630	49	2011-03-22 01:12:55-03	0.0194	123530.01	146618.87	23088.86	4396.02	41	6	Em análise
709	422	49	2022-03-23 06:34:46-03	0.0187	12408.08	16879.89	4471.81	288.54	88	3	Em análise
1282	136	49	2019-07-05 00:25:35-03	0.0242	37365.01	46000.41	8635.40	1065.33	79	5	Em análise
1341	343	49	2016-07-13 16:47:51-03	0.0134	132696.23	191665.21	58968.98	2477.77	95	5	Em análise
1668	710	49	2022-06-26 13:49:25-03	0.0240	18369.72	32359.22	13989.50	495.46	93	5	Em análise
9	148	50	2019-01-23 03:38:42-02	0.0244	14925.18	21038.86	6113.68	397.35	103	2	Em análise
734	928	50	2015-06-22 01:01:44-03	0.0126	99998.08	170949.08	70951.00	17409.00	6	4	Em análise
234	129	51	2014-01-24 17:03:36-02	0.0093	51819.51	82445.73	30626.22	718.52	120	1	Em análise
858	370	51	2018-06-30 10:47:13-03	0.0130	72681.24	97406.91	24725.67	3544.82	24	2	Em análise
829	577	52	2022-11-07 09:31:27-03	0.0188	62615.89	75752.37	13136.48	1594.18	72	5	Em análise
1004	402	52	2012-03-07 20:45:49-03	0.0243	99075.35	149230.21	50154.86	2587.62	111	4	Em análise
1918	929	52	2022-07-15 12:43:58-03	0.0247	126271.18	193413.96	67142.78	3330.27	113	6	Em análise
225	480	53	2013-12-22 06:38:46-02	0.0126	141212.51	171619.62	30407.11	2854.00	78	4	Em análise
419	138	53	2017-10-02 18:31:43-03	0.0109	81705.00	144915.68	63210.68	2434.91	42	4	Em análise
1959	420	53	2010-03-31 23:11:04-03	0.0200	90472.37	177198.48	86726.11	19194.47	5	5	Em análise
184	278	54	2011-12-16 13:53:32-02	0.0188	14460.71	20088.18	5627.47	730.29	25	2	Em análise
193	70	54	2012-07-29 05:37:02-03	0.0098	27901.22	44423.37	16522.15	743.67	47	4	Em análise
295	781	54	2019-05-31 08:50:15-03	0.0162	154432.60	197437.96	43005.36	5474.28	38	2	Em análise
316	522	54	2016-04-27 21:29:52-03	0.0248	69030.06	98722.47	29692.41	2275.00	57	1	Em análise
885	38	54	2010-01-03 05:09:19-02	0.0180	134013.13	240794.24	106781.11	3637.33	61	6	Em análise
1560	876	54	2019-12-19 23:23:33-03	0.0234	91002.52	117029.98	26027.46	3583.27	39	3	Em análise
1871	191	54	2017-07-31 16:50:30-03	0.0133	21672.60	35604.88	13932.28	676.82	42	1	Em análise
8	742	55	2020-07-09 06:52:07-03	0.0181	5279.59	9254.20	3974.61	115.90	97	0	Em análise
853	714	55	2022-02-25 19:29:44-03	0.0171	167446.59	237021.74	69575.15	3520.37	99	6	Em análise
1140	63	55	2020-05-29 02:16:15-03	0.0138	60985.25	77884.05	16898.80	1223.12	85	1	Em análise
1898	661	55	2017-01-17 01:41:12-02	0.0186	112496.49	219120.32	106623.83	2366.37	117	1	Em análise
51	165	56	2019-11-11 23:23:10-03	0.0204	65284.13	82344.28	17060.15	1509.25	106	3	Em análise
1147	853	56	2015-04-13 01:42:04-03	0.0242	43718.45	67193.24	23474.79	4574.46	11	3	Em análise
1649	22	56	2016-04-13 14:42:59-03	0.0216	60844.65	108336.43	47491.78	3384.63	23	2	Em análise
1502	523	57	2021-10-23 23:27:55-03	0.0112	39520.71	62793.15	23272.44	1404.12	34	6	Em análise
33	287	58	2012-02-15 11:07:21-02	0.0191	161524.69	204622.46	43097.77	6016.94	38	5	Em análise
524	39	58	2012-11-15 01:50:33-02	0.0146	145111.82	180996.66	35884.84	5708.68	32	0	Em análise
997	738	58	2010-05-17 06:54:45-03	0.0172	167934.19	204979.60	37045.41	9595.70	21	3	Em análise
1090	549	58	2020-03-16 00:57:53-03	0.0115	17127.83	22575.01	5447.18	316.85	85	1	Em análise
1234	120	58	2017-02-17 01:04:39-02	0.0204	144173.91	188592.72	44418.81	4118.75	62	6	Em análise
1734	819	58	2021-03-07 06:04:04-03	0.0218	40229.92	69161.21	28931.29	994.62	99	0	Em análise
1938	494	58	2012-11-19 10:30:20-02	0.0090	97385.49	166583.93	69198.44	1463.12	102	0	Em análise
167	388	59	2015-11-06 07:02:27-02	0.0209	33766.86	62728.89	28962.03	984.49	61	2	Em análise
662	461	59	2020-10-30 08:57:22-03	0.0213	118546.65	162746.42	44199.77	3361.42	66	2	Em análise
488	824	60	2011-05-05 08:22:58-03	0.0185	73995.04	96178.01	22182.97	1687.10	91	6	Em análise
837	303	60	2018-08-29 13:22:01-03	0.0156	54076.91	91613.30	37536.39	1341.86	64	2	Em análise
936	515	60	2019-09-17 07:22:00-03	0.0123	63882.82	90789.59	26906.77	1036.86	116	5	Em análise
1080	784	60	2017-01-06 19:21:47-02	0.0224	75637.69	104212.88	28575.19	1982.87	87	2	Em análise
1105	545	60	2021-10-31 19:17:07-03	0.0214	82652.12	128271.69	45619.57	2222.95	75	3	Em análise
194	456	61	2018-04-20 14:07:53-03	0.0087	96279.62	115148.01	18868.39	7334.27	14	4	Em análise
470	498	61	2012-03-17 13:35:15-03	0.0208	69989.52	95750.92	25761.40	18416.61	4	6	Em análise
1344	389	61	2010-08-05 00:28:40-03	0.0124	130286.24	212578.03	82291.79	7395.54	20	5	Em análise
1796	871	61	2020-01-13 08:57:25-03	0.0190	41638.41	66154.43	24516.02	902.89	111	2	Em análise
455	999	62	2019-02-10 13:08:36-02	0.0174	135037.07	171017.58	35980.51	137386.71	1	5	Em análise
779	884	62	2018-09-14 12:19:45-03	0.0240	91456.69	137939.39	46482.70	3160.46	50	3	Em análise
1360	958	62	2019-10-25 03:34:56-03	0.0207	20276.13	24323.74	4047.61	1047.11	25	4	Em análise
198	847	63	2018-06-25 01:53:34-03	0.0235	136567.47	244664.78	108097.31	3699.71	87	2	Em análise
329	736	63	2021-11-22 06:11:59-03	0.0138	64264.14	83018.15	18754.01	65150.99	1	6	Em análise
399	248	63	2013-12-19 03:02:55-02	0.0191	9064.30	17544.25	8479.95	214.48	87	1	Em análise
400	520	63	2020-06-02 20:37:39-03	0.0244	15747.06	26179.89	10432.83	451.45	79	2	Em análise
1285	568	63	2021-07-04 20:21:37-03	0.0135	143657.70	243497.22	99839.52	4086.03	48	1	Em análise
1487	136	63	2017-09-11 14:02:28-03	0.0088	146053.18	218806.14	72752.96	2244.90	97	2	Em análise
240	898	64	2016-01-01 23:08:56-02	0.0154	83719.71	156962.58	73242.87	1725.32	90	1	Em análise
475	683	64	2021-11-01 19:06:54-03	0.0132	64259.39	78671.82	14412.43	1376.83	73	6	Em análise
1754	234	64	2021-04-09 02:30:12-03	0.0247	152085.65	190172.16	38086.51	3968.87	120	2	Em análise
423	548	65	2018-12-04 03:17:12-02	0.0094	96822.34	188726.93	91904.59	4077.04	27	6	Em análise
1325	769	65	2015-12-14 12:03:55-02	0.0206	25004.75	46782.48	21777.73	1289.79	25	0	Em análise
1381	834	65	2016-12-18 13:50:43-02	0.0136	136971.80	162117.27	25145.47	2398.24	111	1	Em análise
43	459	66	2022-11-21 11:05:11-03	0.0156	156818.44	196748.06	39929.62	14430.90	12	5	Em análise
137	891	66	2012-04-08 22:06:53-03	0.0132	6107.23	7275.46	1168.23	105.14	111	2	Em análise
848	343	66	2018-07-06 08:08:16-03	0.0224	49839.08	93053.36	43214.28	1632.21	52	2	Em análise
1557	662	66	2019-12-22 08:27:04-03	0.0090	14318.61	24358.26	10039.65	197.47	118	6	Em análise
1897	443	66	2015-07-11 03:06:25-03	0.0247	116292.92	218389.28	102096.36	119165.36	1	4	Em análise
766	451	67	2013-11-19 05:01:54-02	0.0137	95279.11	126793.23	31514.12	4245.51	27	1	Em análise
983	420	68	2018-02-27 00:22:09-03	0.0200	14209.97	25246.25	11036.28	1740.94	9	4	Em análise
1569	659	68	2010-07-18 14:58:52-03	0.0138	104778.42	146684.86	41906.44	5347.84	23	4	Em análise
1585	856	68	2020-10-11 21:32:46-03	0.0210	109688.63	175685.25	65996.62	5088.67	29	3	Em análise
1611	922	68	2018-12-29 16:53:30-02	0.0250	36593.79	47142.69	10548.90	971.63	115	2	Em análise
1183	297	69	2016-12-02 00:48:59-02	0.0198	136515.72	234283.67	97767.95	3248.55	91	2	Em análise
1276	994	69	2015-05-07 03:06:48-03	0.0094	72251.23	106995.24	34744.01	1712.32	54	6	Em análise
378	824	70	2020-11-23 00:22:45-03	0.0210	149166.60	195978.09	46811.49	152299.10	1	2	Em análise
593	355	70	2021-07-28 20:25:13-03	0.0156	167808.50	204293.24	36484.74	4021.37	68	1	Em análise
1817	736	70	2011-11-11 15:23:52-02	0.0181	76155.40	103044.52	26889.12	1665.55	98	6	Em análise
1934	119	70	2014-04-03 02:45:07-03	0.0183	193499.69	240234.39	46734.70	4462.27	87	4	Em análise
1990	164	70	2011-11-18 14:38:58-02	0.0100	44475.10	77662.03	33186.93	1152.56	49	6	Em análise
61	238	71	2011-01-22 08:48:40-02	0.0216	2757.11	4921.74	2164.63	99.08	43	3	Em análise
128	138	71	2010-01-15 13:35:07-02	0.0186	42027.97	57869.25	15841.28	957.42	92	6	Em análise
197	724	71	2018-06-06 05:17:09-03	0.0112	113650.06	143828.50	30178.44	1777.92	113	4	Em análise
495	165	71	2021-05-16 08:54:26-03	0.0225	57332.86	68767.29	11434.43	1409.21	111	3	Em análise
823	510	71	2015-01-18 12:10:10-02	0.0199	113077.12	213904.00	100826.88	2575.57	105	0	Em análise
1189	822	71	2019-10-23 10:32:42-03	0.0212	38738.39	48276.26	9537.87	918.59	107	0	Em análise
1367	670	71	2013-07-03 08:10:21-03	0.0206	27776.13	36728.02	8951.89	952.83	45	6	Em análise
1400	897	71	2012-09-07 06:24:17-03	0.0199	113310.62	177189.69	63879.07	5317.53	28	0	Em análise
1401	66	71	2019-02-14 22:02:45-02	0.0244	14279.96	16877.20	2597.24	989.75	18	3	Em análise
1464	357	71	2017-06-02 13:30:58-03	0.0082	46616.97	63660.16	17043.19	763.75	85	0	Em análise
1708	834	71	2020-04-25 13:06:35-03	0.0085	204394.41	249758.68	45364.27	16673.98	13	1	Em análise
111	85	72	2019-09-01 05:52:49-03	0.0146	68341.84	81810.30	13468.46	1361.99	91	5	Em análise
274	661	72	2012-08-28 04:48:53-03	0.0238	151970.25	179818.86	27848.61	5028.99	54	6	Em análise
679	991	72	2012-02-06 11:36:57-02	0.0207	111890.68	164479.49	52588.81	3794.84	46	0	Em análise
713	164	72	2011-11-27 06:05:50-02	0.0234	135970.24	176825.19	40854.95	21284.47	7	6	Em análise
795	861	72	2021-05-04 14:59:35-03	0.0159	25604.67	31391.85	5787.18	536.93	90	2	Em análise
918	962	72	2021-08-24 04:57:06-03	0.0204	86406.10	154099.71	67693.61	1992.25	107	4	Em análise
991	814	72	2011-12-19 04:05:59-02	0.0238	23544.61	34956.63	11412.02	1017.83	34	3	Em análise
1625	471	72	2021-05-05 06:01:14-03	0.0194	74248.97	99675.37	25426.40	1717.17	95	3	Em análise
319	632	73	2019-07-01 07:04:16-03	0.0103	63096.38	78415.43	15319.05	2106.57	36	0	Em análise
1612	554	73	2019-07-23 02:47:24-03	0.0151	144613.08	174804.35	30191.27	2632.79	118	2	Em análise
277	272	74	2015-07-10 05:49:03-03	0.0221	95979.21	113745.23	17766.02	25335.00	4	3	Em análise
449	778	74	2014-06-10 14:20:52-03	0.0217	45887.56	54695.56	8808.00	1288.75	69	5	Em análise
752	974	74	2015-07-11 07:28:37-03	0.0208	67476.93	83197.19	15720.26	1646.18	93	4	Em análise
958	364	74	2021-12-24 17:22:47-03	0.0243	112903.64	156235.69	43332.05	2928.72	115	2	Em análise
276	731	75	2017-02-04 04:27:26-02	0.0092	10667.70	18799.77	8132.07	244.62	56	5	Em análise
622	885	75	2011-03-07 22:41:47-03	0.0130	46974.86	81837.77	34862.91	1341.99	47	0	Em análise
1131	339	75	2016-01-14 01:23:35-02	0.0204	156131.71	202085.57	45953.86	5196.49	47	6	Em análise
304	237	76	2012-04-01 13:08:05-03	0.0099	102928.92	175083.61	72154.69	1796.70	85	0	Em análise
945	415	76	2022-11-19 04:58:31-03	0.0117	160842.74	220841.00	59998.26	12522.30	14	6	Em análise
1137	805	76	2016-07-13 16:04:21-03	0.0120	126234.06	158179.72	31945.66	5501.32	27	6	Em análise
1782	405	76	2016-01-31 20:25:09-02	0.0237	58212.61	71195.86	12983.25	1508.59	105	6	Em análise
728	807	77	2011-03-05 17:39:47-03	0.0240	175186.65	217450.02	42263.37	4477.13	118	4	Em análise
966	932	77	2011-05-02 07:29:25-03	0.0238	116523.55	143445.00	26921.45	3454.97	69	0	Em análise
1063	284	77	2022-04-12 19:10:23-03	0.0217	118281.38	225897.45	107616.07	2827.64	111	6	Em análise
1717	590	77	2019-02-07 22:42:30-02	0.0231	35631.20	61361.35	25730.15	1196.37	51	6	Em análise
1193	463	78	2018-11-27 15:24:07-02	0.0234	34810.62	49264.08	14453.46	882.26	111	0	Em análise
1372	557	78	2018-01-10 19:52:33-02	0.0199	70642.90	128757.59	58114.69	3075.42	31	3	Em análise
1262	974	79	2017-08-08 18:22:10-03	0.0210	180490.94	245410.40	64919.46	4748.84	77	0	Em análise
1414	467	79	2019-11-21 03:54:43-03	0.0180	77389.43	125685.43	48296.00	5609.84	16	5	Em análise
1885	177	79	2012-05-27 12:00:47-03	0.0191	91789.70	166042.12	74252.42	2236.18	81	1	Em análise
1964	921	79	2010-01-27 03:31:26-02	0.0158	174276.79	205061.49	30784.70	4517.00	60	2	Em análise
110	133	80	2016-06-10 17:05:34-03	0.0130	84783.26	155668.88	70885.62	1687.25	82	3	Em análise
443	37	80	2015-06-09 18:45:29-03	0.0215	58014.12	87004.94	28990.82	1352.64	120	2	Em análise
881	378	80	2017-01-23 21:41:53-02	0.0100	141503.02	223199.06	81696.04	2076.20	115	4	Em análise
1034	85	80	2011-04-19 04:51:00-03	0.0201	24695.16	31300.02	6604.86	560.41	109	2	Em análise
1340	31	80	2016-08-20 08:56:32-03	0.0099	51153.55	82613.69	31460.14	777.32	107	6	Em análise
1353	444	80	2011-08-15 00:54:27-03	0.0164	64833.77	129438.91	64605.14	2753.51	30	1	Em análise
1721	768	80	2013-04-23 20:30:48-03	0.0094	106194.46	182320.84	76126.38	1479.71	120	2	Em análise
1807	861	80	2019-04-20 02:48:59-03	0.0085	13022.14	18606.80	5584.66	227.00	79	4	Em análise
161	602	81	2015-03-28 23:21:30-03	0.0214	170214.62	210891.78	40677.16	3954.16	120	0	Em análise
360	744	81	2021-11-10 01:56:10-03	0.0104	47178.50	57113.38	9934.88	2512.46	21	0	Em análise
862	940	81	2013-10-02 23:30:56-03	0.0126	18484.85	29769.01	11284.16	315.55	107	4	Em análise
1503	775	81	2021-12-27 07:32:26-03	0.0224	54938.94	101545.12	46606.18	3190.19	22	3	Em análise
60	194	82	2012-11-25 06:05:04-02	0.0243	189577.66	243772.06	54194.40	9184.80	29	2	Em análise
1077	181	82	2011-05-23 19:12:07-03	0.0081	69724.34	122830.14	53105.80	1323.03	69	0	Em análise
1196	893	82	2012-05-25 02:00:08-03	0.0133	67637.05	92016.10	24379.05	2326.45	37	1	Em análise
497	763	83	2015-11-09 12:36:16-02	0.0153	94763.22	161314.83	66551.61	4070.60	29	2	Em análise
515	982	84	2017-11-14 17:20:38-02	0.0156	56447.61	96471.98	40024.37	1183.73	88	5	Em análise
1108	716	84	2014-01-01 15:40:10-02	0.0168	135794.46	207700.80	71906.34	3242.16	73	6	Em análise
1149	204	84	2020-07-13 23:41:18-03	0.0244	8810.70	14833.47	6022.77	227.92	119	6	Em análise
1550	814	84	2014-10-26 13:21:31-02	0.0117	25528.13	38960.17	13432.04	436.75	99	1	Em análise
1950	417	84	2010-01-06 00:30:09-02	0.0207	151171.70	221322.36	70150.66	3550.89	104	6	Em análise
227	867	85	2022-08-08 16:57:40-03	0.0163	105738.50	208601.79	102863.29	2307.31	85	0	Em análise
731	660	85	2020-04-11 04:58:18-03	0.0232	136136.96	243044.58	106907.62	3951.92	70	0	Em análise
1384	647	85	2020-10-10 09:35:52-03	0.0140	132774.82	229180.05	96405.23	3067.23	67	6	Em análise
1394	226	85	2014-11-24 04:58:17-02	0.0128	24700.41	42190.52	17490.11	613.13	57	0	Em análise
1402	773	85	2010-08-26 22:47:09-03	0.0143	42692.91	70734.39	28041.48	1126.36	55	1	Em análise
1985	803	85	2022-11-19 21:16:28-03	0.0157	50587.03	69413.01	18825.98	3600.03	16	2	Em análise
1244	92	86	2018-03-25 10:56:25-03	0.0244	59378.08	78516.30	19138.22	1550.55	113	4	Em análise
1737	65	86	2014-07-16 19:00:38-03	0.0188	17028.04	26437.75	9409.71	1598.32	12	3	Em análise
1743	849	86	2012-04-30 05:47:28-03	0.0238	119159.38	210854.80	91695.42	13530.72	10	6	Em análise
675	380	87	2017-09-19 16:23:19-03	0.0121	55826.68	90190.26	34363.58	1768.82	40	4	Em análise
872	141	87	2013-01-28 12:18:55-02	0.0153	60762.11	110392.02	49629.91	3153.82	23	0	Em análise
1654	673	87	2021-10-20 20:40:51-03	0.0170	8983.78	15413.04	6429.26	258.53	53	2	Em análise
1919	526	87	2010-04-03 03:33:42-03	0.0110	17607.49	29858.33	12250.84	297.91	96	3	Em análise
1437	124	88	2010-03-08 05:56:10-03	0.0170	47360.19	70478.49	23118.30	1290.61	58	2	Em análise
1976	216	88	2018-10-29 12:38:48-03	0.0194	69555.41	94909.61	25354.20	1710.03	81	3	Em análise
261	834	89	2016-01-25 11:09:28-02	0.0150	149379.28	205863.25	56483.97	7209.56	25	3	Em análise
653	604	89	2022-01-26 17:53:45-03	0.0145	7494.07	14696.94	7202.87	175.59	67	1	Em análise
974	191	89	2017-12-03 23:56:58-02	0.0203	4059.85	4832.49	772.64	133.17	48	1	Em análise
1660	563	89	2020-12-08 23:05:38-03	0.0103	24663.29	35565.54	10902.25	362.10	118	0	Em análise
179	485	90	2019-12-14 05:55:29-03	0.0151	135021.70	245166.06	110144.36	2458.17	118	3	Em análise
714	71	90	2012-07-06 14:49:44-03	0.0242	100499.86	188600.01	88100.15	2931.73	74	4	Em análise
1041	212	90	2019-11-28 23:00:03-03	0.0248	98973.46	150203.57	51230.11	3345.76	54	2	Em análise
1755	796	90	2011-11-30 15:59:21-02	0.0240	189893.04	242076.45	52183.41	5137.01	92	0	Em análise
1995	765	90	2017-07-12 20:35:07-03	0.0086	26313.94	39247.97	12934.03	491.74	72	5	Em análise
640	164	91	2011-05-17 12:14:39-03	0.0135	85703.61	139655.96	53952.35	1531.69	105	0	Em análise
1071	852	91	2013-05-26 07:06:06-03	0.0229	142169.35	186270.44	44101.09	4417.06	59	6	Em análise
1980	876	91	2010-10-24 06:34:12-02	0.0134	189499.54	242564.46	53064.92	4343.60	66	5	Em análise
300	442	92	2013-10-16 09:56:34-03	0.0111	77895.26	121121.28	43226.02	5053.29	17	6	Em análise
943	876	92	2020-05-18 02:43:38-03	0.0192	111468.69	219863.08	108394.39	2428.83	112	5	Em análise
1148	796	92	2020-08-27 09:35:25-03	0.0202	34039.66	44390.49	10350.83	1192.07	43	5	Em análise
1261	812	92	2022-02-01 14:07:15-03	0.0217	20329.83	34774.07	14444.24	570.96	69	5	Em análise
1777	520	92	2020-08-29 20:59:06-03	0.0223	43392.04	70898.29	27506.25	1093.59	98	2	Em análise
242	582	93	2017-10-22 07:29:39-02	0.0157	87044.83	166828.60	79783.77	2250.33	60	3	Em análise
658	195	93	2011-05-12 21:32:21-03	0.0134	13522.34	23299.20	9776.86	13703.54	1	6	Em análise
1838	385	93	2019-02-02 09:53:57-02	0.0139	8065.50	16074.79	8009.29	205.81	57	6	Em análise
115	463	94	2012-10-26 09:14:32-02	0.0202	63254.60	87764.13	24509.53	1524.84	91	0	Em análise
414	891	94	2020-05-09 19:39:52-03	0.0090	118293.54	229861.15	111567.61	1951.84	88	1	Em análise
865	448	94	2021-08-20 14:48:11-03	0.0229	71614.90	101896.77	30281.87	2711.70	41	4	Em análise
928	859	94	2015-10-08 02:56:25-03	0.0230	44984.14	87246.07	42261.93	1150.35	101	6	Em análise
1539	157	94	2010-05-22 16:19:08-03	0.0106	21563.42	33002.82	11439.40	2087.18	11	3	Em análise
129	11	95	2018-12-27 22:39:46-02	0.0148	112189.30	224260.72	112071.42	2103.65	106	5	Em análise
621	747	95	2015-11-29 19:05:22-02	0.0222	111276.06	180089.89	68813.83	3093.00	73	6	Em análise
1895	596	95	2020-04-28 11:28:54-03	0.0146	16642.62	20329.70	3687.08	523.89	43	2	Em análise
440	94	96	2014-04-01 15:10:00-03	0.0101	111353.93	167371.31	56017.38	19220.54	6	1	Em análise
1040	352	96	2016-08-11 15:19:52-03	0.0190	77344.54	127506.91	50162.37	1920.32	77	0	Em análise
1271	508	96	2013-05-08 23:54:09-03	0.0198	131285.64	237550.40	106264.76	23422.06	6	1	Em análise
1641	966	96	2014-08-06 15:51:13-03	0.0237	54505.62	67276.37	12770.75	1799.83	54	4	Em análise
1643	72	96	2010-10-17 22:39:23-02	0.0201	77521.81	99610.52	22088.71	1722.77	118	2	Em análise
1653	561	96	2011-04-05 09:19:20-03	0.0238	145083.20	245724.28	100641.08	4091.00	79	4	Em análise
1944	173	96	2020-02-10 06:17:49-03	0.0154	42379.89	50439.65	8059.76	902.70	84	0	Em análise
292	718	97	2014-05-24 09:23:07-03	0.0194	150050.16	190522.83	40472.67	5029.35	45	5	Em análise
905	304	97	2011-05-03 23:12:34-03	0.0235	55022.20	71247.88	16225.68	2415.16	33	3	Em análise
944	707	97	2021-08-24 08:04:26-03	0.0147	2837.24	4770.35	1933.11	120.87	29	5	Em análise
1051	186	97	2022-10-29 09:50:30-03	0.0109	45247.52	77025.27	31777.75	1069.97	57	0	Em análise
1116	542	97	2018-12-17 20:54:10-02	0.0230	5994.88	9762.65	3767.77	148.25	117	5	Em análise
1163	644	97	2011-11-24 08:44:25-02	0.0231	75382.98	102354.31	26971.33	2124.53	75	6	Em análise
1287	266	97	2011-07-21 20:43:05-03	0.0091	113835.18	227480.65	113645.47	1717.70	102	5	Em análise
1524	71	97	2014-11-15 00:36:18-02	0.0152	27295.90	52005.89	24709.99	530.50	101	5	Em análise
393	820	98	2017-06-22 09:59:38-03	0.0195	147290.05	186875.66	39585.61	3337.69	102	1	Em análise
676	671	98	2010-07-10 13:36:44-03	0.0109	72726.43	134913.71	62187.28	1813.77	53	1	Em análise
760	800	98	2019-10-24 08:10:11-03	0.0233	139252.04	181592.26	42340.22	36864.22	4	3	Em análise
933	313	98	2019-11-30 14:40:49-03	0.0227	97616.01	125475.13	27859.12	2425.93	109	4	Em análise
25	728	99	2015-07-14 14:59:21-03	0.0116	116949.05	174504.44	57555.39	4395.75	32	5	Em análise
270	748	99	2012-06-24 03:55:49-03	0.0166	20623.87	31283.71	10659.84	709.70	40	0	Em análise
639	417	99	2019-12-20 10:15:29-03	0.0102	14928.43	25153.30	10224.87	415.34	45	6	Em análise
645	674	99	2012-08-22 05:52:00-03	0.0170	150015.05	178999.79	28984.74	3768.21	67	3	Em análise
803	539	99	2013-07-16 20:37:21-03	0.0134	96169.09	154262.02	58092.93	1995.06	78	5	Em análise
1935	491	99	2013-09-03 14:58:01-03	0.0177	162403.24	232794.88	70391.64	4094.86	69	0	Em análise
1940	78	99	2010-10-28 08:28:01-02	0.0164	1815.35	2949.36	1134.01	41.96	76	4	Em análise
1941	711	99	2014-06-05 12:29:52-03	0.0083	21026.93	29591.73	8564.80	289.06	112	3	Em análise
59	60	100	2021-12-31 15:53:53-03	0.0106	113557.05	142096.97	28539.92	3900.44	35	0	Em análise
216	950	100	2018-08-04 20:00:46-03	0.0133	1844.15	2879.96	1035.81	45.31	59	2	Em análise
1181	867	100	2016-10-17 15:54:45-02	0.0137	131801.63	246764.83	114963.20	2781.10	77	6	Em análise
527	575	1	2020-03-03 23:05:20-03	0.0199	57758.43	84849.07	27090.64	5898.28	11	3	Validação documentos
1530	605	1	2010-07-24 10:44:13-03	0.0161	86186.72	105665.09	19478.37	2045.85	71	1	Validação documentos
1888	322	1	2021-10-17 18:56:37-03	0.0215	99871.15	163504.18	63633.03	3003.37	59	6	Validação documentos
1903	966	1	2017-09-14 14:39:27-03	0.0102	31611.25	54606.70	22995.45	660.48	66	6	Validação documentos
1252	785	2	2016-04-09 11:41:37-03	0.0152	108038.21	176965.54	68927.33	3059.81	51	1	Validação documentos
156	531	3	2012-10-15 01:39:20-03	0.0223	130959.31	236890.13	105930.82	23561.42	6	2	Validação documentos
269	683	3	2018-08-17 22:14:08-03	0.0241	201103.08	239843.25	38740.17	6043.31	68	3	Validação documentos
873	941	3	2016-03-12 15:39:59-03	0.0097	67414.91	112788.58	45373.67	1037.96	103	5	Validação documentos
1119	894	3	2010-04-24 10:53:58-03	0.0173	22935.52	36492.52	13557.00	506.94	89	0	Validação documentos
1206	608	3	2017-06-19 05:08:22-03	0.0121	84015.80	136953.90	52938.10	8203.42	11	2	Validação documentos
1392	817	3	2016-06-11 22:30:36-03	0.0176	120328.96	233663.83	113334.87	2638.64	93	5	Validação documentos
1432	715	3	2015-01-17 04:59:11-02	0.0098	119988.93	183409.96	63421.03	1735.95	116	0	Validação documentos
1558	705	3	2015-02-20 07:56:21-02	0.0219	34984.64	41769.25	6784.61	884.06	93	4	Validação documentos
142	431	4	2021-08-02 10:35:59-03	0.0214	163191.44	222265.97	59074.53	29271.68	6	1	Validação documentos
258	892	4	2013-03-21 01:43:58-03	0.0122	149941.78	186102.21	36160.43	2472.94	111	6	Validação documentos
389	182	4	2017-07-22 13:37:16-03	0.0238	39547.97	53003.03	13455.06	2002.20	27	1	Validação documentos
1168	938	4	2011-11-19 09:40:19-02	0.0109	134829.49	180975.48	46145.99	4548.14	36	6	Validação documentos
1926	251	4	2012-01-08 23:18:59-02	0.0086	103227.23	172975.20	69747.97	3346.38	36	1	Validação documentos
1967	500	4	2010-03-26 22:51:33-03	0.0185	78677.16	132774.82	54097.66	1862.22	83	6	Validação documentos
267	183	5	2020-09-03 13:48:29-03	0.0245	50643.82	60087.47	9443.65	4922.19	12	6	Validação documentos
446	314	5	2013-06-03 23:53:19-03	0.0133	42890.56	78296.20	35405.64	945.36	70	6	Validação documentos
1532	365	5	2016-04-03 21:58:07-03	0.0214	32044.13	41368.00	9323.87	775.16	102	3	Validação documentos
266	210	6	2013-11-27 22:40:16-02	0.0186	136246.32	206297.72	70051.40	2894.95	113	1	Validação documentos
573	981	6	2012-12-16 01:56:08-02	0.0165	81430.04	138766.31	57336.27	3017.94	36	6	Validação documentos
672	897	6	2017-07-01 07:56:35-03	0.0182	170663.12	228697.87	58034.75	5046.05	53	0	Validação documentos
1455	925	6	2016-07-19 12:27:45-03	0.0120	33474.67	55191.55	21716.88	1176.93	35	0	Validação documentos
1798	365	6	2021-04-12 09:37:29-03	0.0157	92967.51	136645.37	43677.86	1746.21	116	4	Validação documentos
665	369	7	2010-04-30 04:25:55-03	0.0134	5084.61	6586.85	1502.24	95.45	94	1	Validação documentos
1510	781	7	2018-08-02 20:09:45-03	0.0227	106652.51	166759.20	60106.69	2865.78	83	4	Validação documentos
52	480	8	2018-05-20 03:05:44-03	0.0200	5844.07	8138.55	2294.48	229.28	36	3	Validação documentos
1208	477	8	2019-05-19 10:13:33-03	0.0180	161277.45	243435.50	82158.05	4070.67	70	3	Validação documentos
1146	37	9	2018-07-20 01:46:38-03	0.0149	122743.55	240955.66	118212.11	2202.18	120	1	Validação documentos
1554	274	9	2016-10-15 16:55:58-03	0.0179	34783.91	69257.62	34473.71	925.19	63	1	Validação documentos
68	502	10	2019-10-19 17:19:47-03	0.0177	105883.71	210240.41	104356.70	2513.88	78	6	Validação documentos
191	615	10	2011-10-13 00:30:37-03	0.0116	120976.03	241332.01	120355.98	2329.01	80	1	Validação documentos
555	513	10	2014-02-16 15:04:59-03	0.0120	125318.00	167952.25	42634.25	3747.73	43	6	Validação documentos
1722	576	10	2018-05-01 08:41:20-03	0.0202	148512.46	190244.09	41731.63	26531.21	6	0	Validação documentos
65	715	11	2020-03-30 15:10:19-03	0.0097	135344.86	178804.33	43459.47	2400.67	82	4	Validação documentos
85	236	11	2012-11-18 16:25:42-02	0.0113	52410.16	68065.87	15655.71	842.61	108	4	Validação documentos
484	491	11	2019-06-14 18:30:45-03	0.0124	172495.49	211795.09	39299.60	87855.25	2	1	Validação documentos
820	732	11	2019-08-19 14:58:22-03	0.0162	83530.76	117470.17	33939.41	2145.29	62	3	Validação documentos
1050	891	11	2012-03-10 11:09:44-03	0.0195	16830.04	20661.10	3831.06	376.84	106	1	Validação documentos
278	608	12	2011-05-03 21:30:59-03	0.0156	106979.13	145268.66	38289.53	2187.32	93	2	Validação documentos
606	154	12	2022-11-22 05:50:17-03	0.0125	158370.26	210306.08	51935.82	5886.28	33	3	Validação documentos
1016	774	12	2019-07-19 09:59:16-03	0.0193	133730.12	172878.73	39148.61	9302.42	17	2	Validação documentos
1396	915	12	2020-03-31 19:56:27-03	0.0248	67743.15	131033.88	63290.73	2071.64	68	1	Validação documentos
1770	569	12	2016-11-15 15:40:13-02	0.0134	172309.41	242717.94	70408.53	3499.52	81	0	Validação documentos
1835	485	12	2018-05-10 20:20:30-03	0.0229	106645.85	136905.74	30259.89	3108.93	68	3	Validação documentos
1841	998	12	2010-03-30 13:26:09-03	0.0194	106295.21	170079.07	63783.86	4130.19	36	5	Validação documentos
431	17	13	2017-11-04 01:01:18-02	0.0221	8635.64	11554.57	2918.93	971.97	10	0	Validação documentos
641	966	13	2014-07-09 03:40:31-03	0.0184	67612.24	101067.55	33455.31	1702.05	72	1	Validação documentos
1874	292	13	2011-01-23 01:45:45-02	0.0161	75206.68	128730.70	53524.02	2173.19	51	0	Validação documentos
512	44	14	2010-03-01 17:36:47-03	0.0123	97456.84	160125.10	62668.26	2513.73	53	3	Validação documentos
1833	236	14	2015-01-07 02:25:51-02	0.0203	57867.37	106215.81	48348.44	1369.70	97	2	Validação documentos
1095	908	15	2021-09-16 17:13:33-03	0.0150	19128.15	37501.87	18373.72	462.73	65	5	Validação documentos
1531	27	15	2017-12-11 17:24:01-02	0.0113	9530.23	15679.65	6149.42	212.27	63	3	Validação documentos
1671	247	15	2010-04-02 18:25:41-03	0.0193	10533.78	18464.59	7930.81	240.20	98	4	Validação documentos
464	694	16	2013-12-22 11:25:49-02	0.0196	170732.31	206277.81	35545.50	4104.74	87	2	Validação documentos
1044	715	16	2022-01-18 21:20:20-03	0.0125	23515.68	39177.73	15662.05	541.54	63	3	Validação documentos
294	502	17	2018-01-20 08:06:38-02	0.0128	49060.33	65304.87	16244.54	1395.59	47	2	Validação documentos
684	231	17	2021-10-27 12:10:18-03	0.0200	98442.57	177370.84	78928.27	2314.69	96	6	Validação documentos
251	588	18	2020-06-14 04:39:19-03	0.0123	58117.57	92667.10	34549.53	1071.39	90	3	Validação documentos
812	90	18	2011-09-25 11:22:59-03	0.0250	122410.31	222996.37	100586.06	3905.04	62	4	Validação documentos
971	747	18	2014-01-25 18:15:59-02	0.0235	28339.93	34819.45	6479.52	746.24	96	0	Validação documentos
988	69	18	2012-01-09 18:32:03-02	0.0167	40953.15	70399.33	29446.18	2533.24	19	0	Validação documentos
1000	979	18	2019-07-20 07:13:31-03	0.0236	102423.66	131375.93	28952.27	9240.94	13	4	Validação documentos
1042	275	18	2015-04-28 05:26:36-03	0.0118	29483.43	38574.48	9091.05	1206.45	29	2	Validação documentos
1624	843	18	2018-04-30 20:17:27-03	0.0197	136705.50	170708.60	34003.10	10615.31	15	6	Validação documentos
1849	733	18	2018-07-20 23:27:59-03	0.0125	106117.91	191298.79	85180.88	1765.68	112	2	Validação documentos
70	49	19	2011-12-23 00:03:18-02	0.0100	129370.04	190944.62	61574.58	3406.81	48	6	Validação documentos
411	611	19	2010-01-24 23:48:52-02	0.0153	16101.86	27237.48	11135.62	902.32	21	2	Validação documentos
1593	317	19	2017-12-28 17:30:31-02	0.0135	131054.29	225354.74	94300.45	2276.16	112	1	Validação documentos
1652	506	19	2019-08-24 14:07:55-03	0.0154	141379.94	247982.84	106602.90	3489.35	64	1	Validação documentos
1862	664	19	2018-11-20 04:51:47-02	0.0145	152883.65	192177.16	39293.51	4803.19	43	4	Validação documentos
1890	442	19	2017-12-19 07:44:00-02	0.0194	139973.05	195488.33	55515.28	3132.00	105	2	Validação documentos
1311	243	20	2022-04-03 05:59:36-03	0.0098	140577.88	201068.12	60490.24	4764.10	35	0	Validação documentos
1415	82	20	2018-10-23 06:29:32-03	0.0240	68426.28	104815.19	36388.91	1840.24	94	6	Validação documentos
770	944	21	2015-09-14 13:29:25-03	0.0218	41278.79	67154.45	25875.66	1636.91	37	5	Validação documentos
868	5	21	2010-10-30 02:10:42-02	0.0090	95588.57	173050.34	77461.77	1403.07	106	3	Validação documentos
1012	981	21	2012-09-08 09:56:22-03	0.0193	105583.70	205614.70	100031.00	3070.52	57	2	Validação documentos
1601	107	21	2018-01-21 15:23:59-02	0.0237	38977.17	53357.45	14380.28	4066.90	11	0	Validação documentos
175	135	22	2013-10-05 11:39:58-03	0.0219	26496.31	49172.28	22675.97	1347.44	26	5	Validação documentos
178	694	22	2017-08-19 00:46:19-03	0.0216	67887.72	90968.57	23080.85	1926.60	67	2	Validação documentos
568	817	22	2010-02-27 18:33:41-03	0.0083	1547.45	2178.30	630.85	97.98	17	2	Validação documentos
705	103	22	2017-11-16 06:28:19-02	0.0228	36080.00	60873.64	24793.64	892.49	113	3	Validação documentos
775	449	22	2012-06-17 06:24:23-03	0.0137	9688.83	14446.37	4757.54	217.98	69	5	Validação documentos
1803	677	22	2011-11-16 08:23:55-02	0.0112	155099.95	228671.54	73571.59	2726.75	91	6	Validação documentos
49	916	23	2013-08-30 02:08:32-03	0.0218	16166.08	20300.80	4134.72	394.28	104	1	Validação documentos
168	376	23	2011-12-22 18:22:34-02	0.0131	190416.90	226478.76	36061.86	13269.33	16	3	Validação documentos
1996	152	23	2021-09-19 13:22:25-03	0.0248	27454.48	35170.43	7715.95	725.30	114	4	Validação documentos
12	527	24	2011-04-04 02:16:06-03	0.0176	5129.28	6535.72	1406.44	444.86	13	5	Validação documentos
118	111	24	2019-07-21 07:46:13-03	0.0206	110206.59	174949.50	64742.91	3026.73	68	3	Validação documentos
447	634	24	2017-07-08 00:33:02-03	0.0208	26959.72	34200.08	7240.36	660.09	92	2	Validação documentos
1123	764	24	2020-07-07 22:44:02-03	0.0208	135426.18	188292.24	52866.06	3108.16	115	1	Validação documentos
1182	376	24	2022-06-10 00:54:48-03	0.0126	113718.30	176885.40	63167.10	7473.19	17	4	Validação documentos
1556	58	24	2022-02-24 10:47:33-03	0.0126	158065.16	193160.85	35095.69	2580.51	118	3	Validação documentos
1872	260	24	2019-04-18 02:07:04-03	0.0094	168158.09	234448.17	66290.08	2757.72	91	2	Validação documentos
28	90	25	2012-06-04 07:18:14-03	0.0098	44786.93	60061.01	15274.08	1682.30	31	6	Validação documentos
121	87	25	2017-12-11 02:01:00-02	0.0174	29024.33	53930.86	24906.53	979.79	42	2	Validação documentos
941	429	25	2011-01-22 19:35:33-02	0.0107	30435.38	52108.68	21673.30	30761.04	1	0	Validação documentos
1293	330	25	2015-04-24 03:07:03-03	0.0165	104671.53	208308.58	103637.05	2097.11	106	5	Validação documentos
1517	700	25	2022-07-19 12:54:15-03	0.0144	134358.30	164296.25	29937.95	2441.29	110	6	Validação documentos
1971	462	25	2019-04-29 00:16:45-03	0.0201	48587.75	82752.85	34165.10	2227.24	29	5	Validação documentos
81	279	26	2010-09-02 13:34:41-03	0.0137	65800.08	111918.44	46118.36	1535.55	65	3	Validação documentos
575	778	26	2012-09-04 22:27:21-03	0.0113	131018.79	196110.87	65092.08	2406.43	85	5	Validação documentos
1138	425	26	2013-02-20 03:54:00-03	0.0154	175996.63	214598.60	38601.97	8265.78	26	2	Validação documentos
1470	660	26	2017-07-05 16:19:38-03	0.0178	41604.71	83041.15	41436.44	848.21	117	3	Validação documentos
1578	527	26	2022-06-21 15:40:39-03	0.0093	173305.89	224957.59	51651.70	12431.88	15	3	Validação documentos
1878	521	26	2011-02-11 23:51:54-02	0.0107	83570.50	141770.03	58199.53	5710.84	16	2	Validação documentos
1968	556	26	2011-07-19 15:54:19-03	0.0130	36263.89	45593.80	9329.91	702.89	86	0	Validação documentos
38	427	27	2015-05-24 22:27:11-03	0.0103	35424.00	56750.68	21326.68	559.64	103	3	Validação documentos
712	138	27	2010-08-26 03:06:59-03	0.0222	147228.59	193295.68	46067.09	4271.20	66	0	Validação documentos
1774	347	27	2016-11-08 19:51:46-02	0.0197	158689.09	234716.44	76027.35	3489.19	116	2	Validação documentos
1848	444	27	2011-04-30 02:43:17-03	0.0224	40573.22	61140.97	20567.75	978.97	119	1	Validação documentos
1883	476	27	2014-09-12 09:29:34-03	0.0246	29530.20	35056.88	5526.68	2871.85	12	4	Validação documentos
10	413	28	2020-03-28 23:08:32-03	0.0147	149661.37	192346.80	42685.43	2993.29	91	6	Validação documentos
262	122	28	2011-04-14 00:27:11-03	0.0171	81836.82	104094.88	22258.06	1701.16	102	5	Validação documentos
297	746	28	2011-09-30 06:54:13-03	0.0209	105405.19	166354.02	60948.83	2780.20	76	0	Validação documentos
354	662	28	2020-09-01 18:50:56-03	0.0151	122141.83	242789.63	120647.80	2752.23	74	4	Validação documentos
916	457	28	2021-02-11 07:11:51-03	0.0095	155359.45	221739.32	66379.87	4999.72	37	2	Validação documentos
1169	115	28	2018-08-30 04:16:52-03	0.0165	132221.19	227756.23	95535.04	2953.50	82	2	Validação documentos
1840	769	28	2022-11-09 18:15:07-03	0.0207	133660.50	197742.96	64082.46	4724.38	43	3	Validação documentos
534	215	29	2014-12-24 21:44:31-02	0.0219	1493.21	1923.68	430.47	142.85	12	2	Validação documentos
583	600	29	2015-05-30 10:10:27-03	0.0150	91583.79	119903.60	28319.81	2157.74	68	4	Validação documentos
957	604	29	2013-07-28 21:26:58-03	0.0189	23178.34	36317.80	13139.46	535.53	91	4	Validação documentos
1336	185	29	2013-01-08 17:54:38-02	0.0154	26426.44	41567.94	15141.50	1281.59	25	6	Validação documentos
1349	579	29	2016-11-03 13:38:21-02	0.0171	48549.13	87596.50	39047.37	4196.72	13	4	Validação documentos
1516	933	29	2020-09-12 15:09:07-03	0.0096	89242.32	159243.81	70001.49	2643.33	41	3	Validação documentos
1907	52	29	2019-07-22 13:57:19-03	0.0147	93841.61	164887.85	71046.24	6278.79	17	5	Validação documentos
1974	897	29	2016-06-20 22:12:41-03	0.0232	13329.86	16882.67	3552.81	780.48	22	6	Validação documentos
346	163	30	2020-02-17 14:57:46-03	0.0213	112916.10	143911.36	30995.26	2862.65	87	1	Validação documentos
623	584	30	2011-11-20 11:17:07-02	0.0174	31351.03	45070.30	13719.27	1557.22	25	5	Validação documentos
1162	812	30	2011-11-30 12:01:58-02	0.0102	162134.06	214281.02	52146.96	2800.17	88	5	Validação documentos
1599	794	30	2011-09-14 22:49:26-03	0.0228	112893.18	160768.12	47874.94	8971.07	15	1	Validação documentos
1605	360	30	2022-06-02 02:55:25-03	0.0108	69184.68	106081.03	36896.35	1077.86	110	2	Validação documentos
951	839	31	2016-08-25 07:58:09-03	0.0184	85930.34	112946.38	27016.04	1944.45	92	3	Validação documentos
1963	392	31	2019-09-14 10:24:18-03	0.0242	66105.03	96572.10	30467.07	1737.51	106	3	Validação documentos
1984	588	31	2013-05-27 19:33:23-03	0.0249	101739.98	193983.64	92243.66	5089.53	28	0	Validação documentos
40	182	32	2013-03-06 04:07:34-03	0.0176	79874.02	99049.63	19175.61	4109.09	24	1	Validação documentos
403	581	32	2010-12-02 02:07:07-02	0.0092	10920.92	15667.98	4747.06	261.29	53	3	Validação documentos
490	548	32	2013-04-02 02:55:14-03	0.0154	53880.42	97323.94	43443.52	1513.38	52	5	Validação documentos
743	511	32	2015-10-17 15:24:42-03	0.0116	147211.86	200167.25	52955.39	7938.93	21	1	Validação documentos
964	949	32	2018-05-01 19:41:10-03	0.0209	16570.47	27458.76	10888.29	458.70	68	5	Validação documentos
968	103	32	2013-06-21 05:12:05-03	0.0130	93779.88	115429.08	21649.20	1706.72	97	0	Validação documentos
1583	160	32	2021-01-27 00:14:46-03	0.0087	57813.79	106288.07	48474.28	6706.42	9	0	Validação documentos
889	246	33	2016-04-02 05:31:07-03	0.0177	103783.51	175148.39	71364.88	3364.76	45	5	Validação documentos
1689	816	33	2014-11-06 04:59:00-02	0.0122	101627.79	124087.64	22459.85	3759.54	33	5	Validação documentos
371	294	34	2016-01-18 07:17:32-02	0.0225	18580.96	24331.13	5750.17	901.65	28	0	Validação documentos
1180	750	34	2010-10-21 00:12:09-02	0.0193	102268.52	158960.17	56691.65	3098.91	53	0	Validação documentos
1288	471	34	2021-03-25 15:41:06-03	0.0137	71697.99	118154.95	46456.96	1673.19	65	0	Validação documentos
1787	391	34	2010-12-16 12:19:59-02	0.0169	54972.16	83383.05	28410.89	1479.43	59	6	Validação documentos
143	519	35	2015-05-10 10:26:19-03	0.0124	69325.85	101029.22	31703.37	2511.36	34	4	Validação documentos
574	396	35	2011-01-29 04:02:10-02	0.0115	72222.75	87211.10	14988.35	1536.76	68	1	Validação documentos
1692	18	35	2013-06-15 05:53:20-03	0.0174	58403.13	83810.27	25407.14	1495.07	66	4	Validação documentos
71	804	36	2010-05-15 07:40:14-03	0.0228	66329.20	98562.45	32233.25	14186.86	5	4	Validação documentos
473	462	36	2013-07-07 01:08:36-03	0.0140	66532.94	128266.34	61733.40	4207.22	18	6	Validação documentos
987	815	36	2022-12-16 12:39:28-03	0.0110	118495.69	203830.96	85335.27	2137.87	86	1	Validação documentos
1079	569	36	2017-08-30 11:01:22-03	0.0097	41168.58	52200.81	11032.23	660.99	96	3	Validação documentos
1684	868	36	2020-12-20 22:38:42-03	0.0160	124537.07	172060.29	47523.22	4886.84	33	4	Validação documentos
1892	112	36	2017-10-02 01:39:32-03	0.0192	42179.44	54439.24	12259.80	923.89	110	0	Validação documentos
131	694	37	2021-01-17 06:28:43-03	0.0222	101045.70	183975.03	82929.33	7572.35	16	4	Validação documentos
172	381	37	2020-08-30 07:32:52-03	0.0186	90424.09	135771.10	45347.01	1897.54	118	1	Validação documentos
468	961	37	2015-07-31 06:42:43-03	0.0250	111860.21	209814.23	97954.02	3382.42	71	4	Validação documentos
494	745	37	2015-11-03 06:31:45-02	0.0099	77031.17	124517.06	47485.89	1246.90	96	1	Validação documentos
761	434	37	2011-04-19 04:07:17-03	0.0221	66852.92	104830.16	37977.24	2198.50	51	3	Validação documentos
1832	948	37	2015-05-20 23:12:40-03	0.0203	41083.99	60138.63	19054.64	2881.85	17	6	Validação documentos
1867	650	37	2022-09-29 09:09:04-03	0.0223	44931.24	62882.53	17951.29	1103.94	108	0	Validação documentos
1925	995	37	2022-03-11 03:51:56-03	0.0235	44324.38	75272.77	30948.39	2516.67	23	5	Validação documentos
1966	679	37	2011-02-24 14:27:13-03	0.0215	149294.23	178817.54	29523.31	4489.64	59	4	Validação documentos
222	173	38	2019-02-09 21:03:58-02	0.0233	137141.03	166060.29	28919.26	6900.48	27	2	Validação documentos
480	560	38	2011-11-03 02:11:56-02	0.0083	70831.89	107264.52	36432.63	1204.60	81	3	Validação documentos
710	845	38	2012-07-13 23:25:07-03	0.0099	149926.14	217880.51	67954.37	2328.32	103	1	Validação documentos
890	685	38	2021-07-09 17:15:50-03	0.0159	12091.01	16030.62	3939.61	236.71	106	2	Validação documentos
929	922	38	2021-08-09 04:49:06-03	0.0229	128793.36	165356.94	36563.58	3190.87	114	6	Validação documentos
1023	195	38	2014-09-07 13:31:55-03	0.0220	92616.13	127684.77	35068.64	5355.72	22	5	Validação documentos
18	909	39	2018-11-14 11:53:23-02	0.0209	50519.77	68706.61	18186.84	1461.12	62	3	Validação documentos
395	724	39	2010-07-26 15:57:38-03	0.0150	30797.01	47054.31	16257.30	4667.48	7	3	Validação documentos
1954	391	39	2019-10-07 03:05:23-03	0.0198	111634.40	178116.70	66482.30	4135.34	39	0	Validação documentos
1216	239	40	2019-08-14 11:15:14-03	0.0187	77700.32	137032.72	59332.40	2028.46	68	2	Validação documentos
1438	261	40	2017-02-27 19:52:51-03	0.0201	72067.26	138914.27	66847.01	1688.75	98	2	Validação documentos
1595	248	40	2022-08-19 19:29:01-03	0.0220	4063.96	5436.18	1372.22	275.86	18	0	Validação documentos
1808	745	40	2016-05-23 22:31:09-03	0.0214	23485.88	46646.94	23161.06	580.22	95	5	Validação documentos
407	793	41	2018-05-04 21:13:50-03	0.0110	83168.98	148156.91	64987.93	1634.30	75	3	Validação documentos
459	746	41	2013-11-27 08:52:15-02	0.0150	5419.38	7083.25	1663.87	122.66	73	2	Validação documentos
1738	219	41	2021-05-22 05:30:28-03	0.0177	148260.38	235254.36	86993.98	3923.15	63	3	Validação documentos
74	521	42	2011-01-15 11:38:20-02	0.0149	183802.99	225791.78	41988.79	3646.73	94	6	Validação documentos
136	658	42	2011-12-21 08:16:28-02	0.0246	29567.91	52244.44	22676.53	1247.44	36	3	Validação documentos
315	183	42	2015-06-16 10:55:34-03	0.0153	137071.42	165345.14	28273.72	3748.04	54	5	Validação documentos
648	794	42	2020-11-30 07:49:07-03	0.0095	115351.00	211447.42	96096.42	2157.45	75	1	Validação documentos
1084	126	42	2022-08-08 20:34:41-03	0.0221	133629.73	165005.29	31375.56	3645.45	76	1	Validação documentos
1351	71	42	2021-02-12 06:06:07-03	0.0154	80735.34	154852.70	74117.36	2071.29	60	5	Validação documentos
1511	929	42	2016-04-05 15:49:28-03	0.0145	149434.98	233106.08	83671.10	3357.79	72	6	Validação documentos
592	379	43	2016-02-29 15:31:24-03	0.0225	53675.36	99134.66	45459.30	2479.77	30	0	Validação documentos
745	858	43	2014-10-29 19:51:49-02	0.0246	61551.75	89339.63	27787.88	1797.69	76	6	Validação documentos
781	103	43	2021-08-17 14:31:09-03	0.0214	11453.40	14530.19	3076.79	274.86	105	0	Validação documentos
949	560	43	2016-05-04 08:50:24-03	0.0121	76701.28	123000.84	46299.56	3573.84	25	1	Validação documentos
1088	358	43	2014-03-28 00:47:41-03	0.0202	67799.94	112570.98	44771.04	1647.41	89	3	Validação documentos
1407	667	43	2010-07-06 23:02:57-03	0.0094	66156.24	89567.96	23411.72	1335.25	67	0	Validação documentos
1676	924	43	2017-07-26 08:23:19-03	0.0170	118131.86	192952.64	74820.78	3323.16	55	3	Validação documentos
587	266	44	2021-03-31 23:29:52-03	0.0137	76609.38	150537.10	73927.72	1770.97	66	1	Validação documentos
664	971	44	2013-02-08 13:10:39-02	0.0134	18178.53	34721.08	16542.55	306.46	119	0	Validação documentos
776	744	44	2022-08-02 19:51:19-03	0.0121	63088.85	93017.02	29928.17	1010.86	117	3	Validação documentos
1631	784	44	2018-06-03 14:54:50-03	0.0178	7629.91	10451.96	2822.05	199.03	65	6	Validação documentos
1656	682	44	2020-04-28 07:43:12-03	0.0223	11145.28	15777.05	4631.77	11393.82	1	0	Validação documentos
1779	245	44	2012-10-02 12:36:26-03	0.0238	27959.51	48966.22	21006.71	1208.69	34	1	Validação documentos
31	997	45	2018-04-29 04:44:00-03	0.0096	112587.57	140514.26	27926.69	1636.95	113	4	Validação documentos
257	690	45	2018-10-30 20:58:47-03	0.0172	176449.99	240493.02	64043.03	3655.33	104	0	Validação documentos
281	723	45	2013-01-10 09:30:48-02	0.0102	140906.90	209022.44	68115.54	3920.34	45	2	Validação documentos
1018	840	45	2015-08-01 12:02:58-03	0.0248	122930.67	175352.43	52421.76	12906.25	11	5	Validação documentos
1439	999	45	2019-04-27 06:26:45-03	0.0230	142746.74	187878.54	45131.80	4012.13	75	4	Validação documentos
1773	27	45	2022-09-20 11:53:47-03	0.0096	72508.41	112191.98	39683.57	5959.54	13	1	Validação documentos
75	504	46	2010-05-22 10:35:02-03	0.0107	53891.91	82607.67	28715.76	799.59	120	5	Validação documentos
298	475	46	2015-03-28 05:52:15-03	0.0128	81338.81	115991.17	34652.36	1782.10	69	4	Validação documentos
452	43	46	2022-06-20 20:44:00-03	0.0099	19728.66	29913.81	10185.15	1624.83	13	1	Validação documentos
1998	657	46	2018-12-05 23:01:28-02	0.0166	179835.53	217734.40	37898.87	4007.08	83	4	Validação documentos
42	661	47	2011-03-06 03:44:23-03	0.0135	68016.90	82315.96	14299.06	2507.85	34	3	Validação documentos
192	594	47	2016-06-06 21:12:45-03	0.0182	152375.39	249035.39	96660.00	7408.36	26	3	Validação documentos
864	378	47	2013-12-16 14:14:48-02	0.0158	79873.74	103169.64	23295.90	2709.10	40	3	Validação documentos
1083	820	47	2012-09-06 21:10:10-03	0.0155	105206.31	194504.44	89298.13	1965.96	115	6	Validação documentos
1155	707	47	2011-04-29 22:21:13-03	0.0087	15848.66	19948.27	4099.61	218.62	115	5	Validação documentos
1478	637	47	2014-01-15 08:08:53-02	0.0138	125713.57	172106.20	46392.63	2537.19	84	3	Validação documentos
1697	564	47	2022-02-14 00:38:55-03	0.0223	55592.06	107539.76	51947.70	1382.27	103	2	Validação documentos
1745	424	47	2017-08-27 06:57:32-03	0.0234	12940.98	20831.83	7890.85	517.83	38	6	Validação documentos
275	332	48	2017-08-07 16:30:22-03	0.0166	46387.89	88725.05	42337.16	978.15	94	0	Validação documentos
361	265	48	2016-10-28 23:44:48-02	0.0189	100862.14	153122.88	52260.74	7363.94	16	3	Validação documentos
1223	279	48	2016-07-13 04:46:49-03	0.0146	43924.50	59711.93	15787.43	9173.40	5	5	Validação documentos
1450	920	48	2014-11-08 23:19:32-02	0.0140	3656.33	4440.22	783.89	75.75	81	6	Validação documentos
350	738	49	2014-03-28 09:35:12-03	0.0171	106319.84	210478.52	104158.68	2656.82	68	6	Validação documentos
409	467	49	2014-07-10 22:42:45-03	0.0096	35554.28	60545.37	24991.09	1454.14	28	2	Validação documentos
559	375	49	2022-09-19 00:36:25-03	0.0213	103759.69	158635.86	54876.17	3708.34	43	6	Validação documentos
927	371	49	2010-10-20 19:02:24-02	0.0222	64101.70	81144.26	17042.56	3714.53	22	3	Validação documentos
1154	326	49	2014-08-02 22:53:03-03	0.0111	166623.72	228895.44	62271.72	2550.52	117	2	Validação documentos
1220	917	49	2020-08-02 22:49:36-03	0.0140	91234.45	180077.48	88843.03	1733.65	96	5	Validação documentos
1751	109	49	2015-05-31 04:00:45-03	0.0180	4285.81	7194.61	2908.80	120.86	57	5	Validação documentos
1295	927	50	2022-01-28 07:40:18-03	0.0123	157633.95	199276.92	41642.97	2600.14	112	3	Validação documentos
1572	950	50	2017-06-09 21:24:36-03	0.0124	96023.28	148429.81	52406.53	1559.48	117	5	Validação documentos
778	452	51	2015-11-24 03:43:38-02	0.0248	107064.71	137822.99	30758.28	4990.40	31	3	Validação documentos
1741	295	51	2020-09-07 13:30:31-03	0.0083	9775.19	19046.08	9270.89	146.15	98	3	Validação documentos
1910	997	51	2013-06-10 10:45:19-03	0.0181	18534.02	24177.91	5643.89	768.11	32	0	Validação documentos
613	737	52	2016-08-15 15:56:29-03	0.0117	154962.50	206269.97	51307.47	2971.11	81	5	Validação documentos
898	82	52	2013-06-06 22:07:57-03	0.0107	107944.66	186552.83	78608.17	1940.13	85	2	Validação documentos
1015	120	52	2015-05-04 08:35:01-03	0.0124	75962.67	112835.80	36873.13	3227.74	28	3	Validação documentos
279	517	53	2012-01-25 03:11:57-02	0.0086	84614.58	162394.06	77779.48	2235.01	46	0	Validação documentos
312	942	53	2016-04-13 08:27:07-03	0.0180	116670.34	178652.68	61982.34	3290.21	57	5	Validação documentos
373	548	53	2011-02-17 20:45:36-02	0.0222	98283.60	139798.65	41515.05	2401.18	109	4	Validação documentos
940	991	53	2013-05-05 05:02:03-03	0.0094	30032.33	36866.80	6834.47	575.95	72	5	Validação documentos
1320	5	53	2012-12-19 15:27:53-02	0.0159	59852.05	79229.78	19377.73	2592.21	29	5	Validação documentos
1768	542	53	2012-05-23 21:09:06-03	0.0247	102826.16	134778.99	31952.83	6846.14	19	5	Validação documentos
1078	395	54	2014-02-19 00:14:51-03	0.0244	39138.66	49429.78	10291.12	20288.44	2	5	Validação documentos
1627	651	54	2010-09-11 13:37:09-03	0.0229	102199.63	152867.12	50667.49	2579.76	105	1	Validação documentos
1677	881	54	2022-04-06 01:53:30-03	0.0117	130962.24	236449.64	105487.40	2162.43	106	5	Validação documentos
1748	7	54	2013-09-23 06:38:22-03	0.0187	28240.99	41620.86	13379.87	947.36	44	6	Validação documentos
1047	166	55	2018-02-13 21:05:51-02	0.0141	25122.99	32393.19	7270.20	1032.80	30	5	Validação documentos
1225	589	55	2010-02-12 18:15:46-02	0.0169	79199.41	157968.47	78769.06	1634.22	102	4	Validação documentos
1943	937	55	2010-04-15 10:27:17-03	0.0119	160604.93	216225.23	55620.30	3599.40	64	0	Validação documentos
237	862	56	2018-05-15 00:08:54-03	0.0110	78874.82	115096.34	36221.52	1944.94	54	1	Validação documentos
238	201	56	2020-03-29 08:46:53-03	0.0189	71355.92	90989.60	19633.68	1529.58	114	6	Validação documentos
507	52	56	2012-04-24 00:31:44-03	0.0151	89815.73	144180.40	54364.67	1882.95	85	3	Validação documentos
589	319	56	2017-11-23 22:10:02-02	0.0115	75760.50	108888.98	33128.48	2000.81	50	2	Validação documentos
753	867	56	2020-10-17 01:01:09-03	0.0118	71643.04	94801.74	23158.70	2350.44	38	3	Validação documentos
1520	510	56	2012-10-14 17:52:48-03	0.0180	112487.74	224818.40	112330.66	4359.84	35	6	Validação documentos
1891	188	56	2016-11-18 05:56:20-02	0.0139	90814.59	160733.90	69919.31	3156.13	37	0	Validação documentos
196	818	57	2017-03-14 18:57:22-03	0.0084	71001.68	140713.99	69712.31	1201.54	82	5	Validação documentos
1167	204	57	2014-06-18 07:14:22-03	0.0111	132997.94	170799.29	37801.35	2233.34	98	3	Validação documentos
911	124	58	2019-05-26 18:22:38-03	0.0088	23833.83	28268.19	4434.36	332.03	114	0	Validação documentos
1151	200	58	2015-05-19 04:38:06-03	0.0230	41882.61	55429.71	13547.10	1523.44	44	1	Validação documentos
1441	342	58	2021-02-26 18:19:33-03	0.0239	35917.27	55694.86	19777.59	968.70	92	4	Validação documentos
657	782	59	2015-11-04 14:57:19-02	0.0158	100683.18	172385.57	71702.39	1963.49	106	4	Validação documentos
846	817	59	2020-06-02 07:09:57-03	0.0166	75574.24	92324.12	16749.88	2750.01	37	3	Validação documentos
1001	382	59	2022-11-22 21:22:52-03	0.0173	48982.13	63327.36	14345.23	1403.08	54	6	Validação documentos
1173	484	59	2016-12-10 18:34:20-02	0.0080	98816.89	164708.79	65891.90	1324.58	114	0	Validação documentos
271	371	60	2017-03-07 10:37:47-03	0.0115	98377.84	194572.02	96194.18	2423.53	55	4	Validação documentos
541	975	60	2019-05-14 05:49:23-03	0.0172	155677.25	195077.87	39400.62	3107.45	116	2	Validação documentos
171	287	61	2012-08-25 03:18:44-03	0.0202	65243.90	81561.96	16318.06	1549.75	95	5	Validação documentos
732	446	61	2018-01-27 16:07:51-02	0.0232	3643.83	4746.41	1102.58	101.04	79	6	Validação documentos
847	340	61	2012-01-13 05:08:25-02	0.0216	147145.05	242003.35	94858.30	10433.92	17	6	Validação documentos
871	204	61	2015-07-24 14:40:27-03	0.0112	111999.04	141174.23	29175.19	4989.16	26	5	Validação documentos
1202	727	61	2013-07-10 18:51:09-03	0.0196	78432.30	151023.86	72591.56	7398.32	12	2	Validação documentos
1364	106	61	2018-01-07 17:05:43-02	0.0223	172984.90	216795.47	43810.57	4521.21	87	6	Validação documentos
1389	816	61	2013-04-05 13:23:26-03	0.0176	23214.49	41458.00	18243.51	470.79	116	6	Validação documentos
1637	624	61	2016-08-06 22:58:15-03	0.0081	67980.47	111265.80	43285.33	2184.61	36	6	Validação documentos
617	410	62	2014-09-26 11:31:38-03	0.0151	19352.04	37225.21	17873.17	439.33	73	5	Validação documentos
695	583	62	2021-11-03 12:35:10-03	0.0203	155637.62	191034.54	35396.92	9176.80	21	4	Validação documentos
817	974	62	2015-02-23 06:09:08-03	0.0169	37342.07	46838.35	9496.28	1157.75	47	1	Validação documentos
1297	736	62	2019-01-14 18:56:39-02	0.0182	78684.07	96060.85	17376.78	1621.64	119	4	Validação documentos
1412	29	62	2016-05-27 13:21:41-03	0.0133	89540.98	108788.44	19247.46	1712.27	90	4	Validação documentos
1858	756	62	2019-07-30 21:16:19-03	0.0198	95566.49	122175.94	26609.45	6363.28	18	1	Validação documentos
17	543	63	2019-05-27 03:18:08-03	0.0237	35834.33	65794.08	29959.75	907.86	117	1	Validação documentos
160	609	63	2014-01-29 09:33:43-02	0.0135	12591.13	17199.61	4608.48	301.08	62	5	Validação documentos
356	102	63	2019-04-01 12:30:13-03	0.0089	158206.13	224611.67	66405.54	3757.20	53	5	Validação documentos
663	498	63	2015-05-23 23:17:25-03	0.0249	165367.65	247848.06	82480.41	12657.14	16	1	Validação documentos
727	457	63	2016-10-03 02:45:39-03	0.0111	92949.74	163489.66	70539.92	1719.65	83	3	Validação documentos
894	260	63	2010-05-30 19:47:11-03	0.0126	90752.09	128050.14	37298.05	7111.48	14	2	Validação documentos
1399	331	63	2022-08-10 16:11:34-03	0.0182	146571.59	247210.52	100638.93	4239.91	55	2	Validação documentos
499	378	64	2019-10-05 01:24:14-03	0.0204	43067.45	53713.47	10646.02	14945.47	3	2	Validação documentos
764	693	64	2019-06-04 19:03:24-03	0.0127	94354.88	121454.58	27099.70	2042.71	70	5	Validação documentos
985	123	64	2010-05-31 19:39:53-03	0.0114	54182.11	68353.22	14171.11	942.36	94	6	Validação documentos
1075	647	64	2011-10-03 02:49:21-03	0.0139	28527.40	41765.89	13238.49	651.27	68	3	Validação documentos
1111	866	64	2020-02-29 18:24:08-03	0.0162	170587.19	205008.03	34420.84	4227.09	66	1	Validação documentos
1192	258	64	2020-10-29 01:45:35-03	0.0098	79972.88	128911.67	48938.79	1360.47	88	0	Validação documentos
1986	767	64	2022-06-20 23:05:33-03	0.0123	107348.68	182012.52	74663.84	3288.05	42	2	Validação documentos
1992	772	64	2016-06-19 01:40:32-03	0.0134	48615.20	84580.69	35965.49	2671.36	21	6	Validação documentos
466	429	65	2012-11-30 20:23:32-02	0.0165	176947.37	243282.66	66335.29	6427.96	37	4	Validação documentos
474	247	65	2018-06-14 11:50:52-03	0.0187	160135.89	194338.25	34202.36	3539.35	101	6	Validação documentos
720	792	65	2013-09-08 04:06:22-03	0.0152	160577.96	202934.93	42356.97	4805.87	47	4	Validação documentos
797	303	65	2019-07-22 19:34:36-03	0.0195	83840.04	141952.08	58112.04	1817.43	119	1	Validação documentos
1298	427	65	2014-09-22 14:06:45-03	0.0223	80972.79	138074.27	57101.48	1976.58	111	1	Validação documentos
1811	772	65	2018-05-30 20:49:59-03	0.0165	126627.65	194956.96	68329.31	5385.47	30	2	Validação documentos
35	72	66	2020-04-05 22:33:38-03	0.0112	4518.38	5668.80	1150.42	194.85	27	6	Validação documentos
1052	519	66	2017-01-15 19:55:52-02	0.0122	134593.16	185262.80	50669.64	3779.70	47	4	Validação documentos
1072	650	66	2012-11-08 15:36:52-02	0.0170	5222.65	9443.42	4220.77	703.75	8	3	Validação documentos
1480	453	66	2010-12-22 21:27:57-02	0.0189	37387.58	66916.64	29529.06	3270.69	13	3	Validação documentos
1576	843	66	2012-09-07 17:07:09-03	0.0131	142097.95	227374.90	85276.95	7783.88	21	3	Validação documentos
1710	483	66	2015-05-16 05:45:18-03	0.0128	107807.31	215518.86	107711.55	1763.15	120	3	Validação documentos
1978	443	66	2021-07-12 17:46:07-03	0.0095	42019.74	51632.31	9612.57	771.90	77	1	Validação documentos
1997	100	66	2010-10-13 17:57:25-03	0.0243	114705.96	183412.54	68706.58	3030.98	105	2	Validação documentos
384	521	67	2015-08-17 17:53:25-03	0.0165	126905.43	198751.32	71845.89	3747.18	50	1	Validação documentos
1003	510	67	2011-04-12 10:03:18-03	0.0169	53343.51	90077.73	36734.22	4310.72	14	4	Validação documentos
1506	967	67	2015-04-09 22:24:20-03	0.0164	21632.99	31454.17	9821.18	1548.20	16	0	Validação documentos
1712	398	67	2014-04-18 00:30:23-03	0.0248	62315.23	77901.30	15586.07	3989.80	20	3	Validação documentos
44	783	68	2010-08-28 23:38:52-03	0.0125	161718.07	215887.80	54169.73	10618.45	17	1	Validação documentos
245	253	68	2020-07-03 06:25:31-03	0.0162	19945.24	23755.74	3810.50	4184.99	5	4	Validação documentos
341	903	68	2016-10-11 14:58:51-03	0.0114	121703.01	154883.11	33180.10	123090.43	1	2	Validação documentos
692	315	68	2020-08-22 19:06:53-03	0.0155	136288.52	205051.67	68763.15	2531.02	117	4	Validação documentos
931	39	68	2013-02-12 07:17:56-02	0.0201	62881.00	120235.95	57354.95	1499.51	93	2	Validação documentos
1324	545	68	2022-01-14 01:55:43-03	0.0150	98895.85	188489.14	89593.29	3575.32	36	5	Validação documentos
1419	104	68	2021-06-25 00:25:39-03	0.0221	1208.69	1608.90	400.21	30.95	91	4	Validação documentos
1592	357	68	2020-02-08 14:56:18-03	0.0109	179134.54	239219.58	60085.04	10016.99	20	6	Validação documentos
502	445	69	2020-09-06 01:47:20-03	0.0222	24748.49	40543.55	15795.06	8618.46	3	3	Validação documentos
659	520	69	2020-05-03 00:03:39-03	0.0120	20736.80	35307.54	14570.74	1617.95	14	3	Validação documentos
990	496	69	2014-08-17 23:42:54-03	0.0089	111385.36	146883.76	35498.40	22875.38	5	5	Validação documentos
1091	967	69	2014-12-02 15:51:49-02	0.0085	90643.97	136585.04	45941.07	1552.73	81	4	Validação documentos
1789	467	69	2013-12-07 06:40:25-02	0.0185	119494.48	172453.13	52958.65	3200.97	64	1	Validação documentos
1953	451	70	2019-11-18 15:49:06-03	0.0146	76753.48	132048.13	55294.65	1451.55	102	1	Validação documentos
772	67	71	2014-06-15 10:44:56-03	0.0204	172345.42	225537.64	53192.22	4066.60	99	6	Validação documentos
1056	857	71	2012-10-17 09:05:28-03	0.0233	7978.49	13850.05	5871.56	212.65	90	0	Validação documentos
1086	697	71	2010-11-30 08:23:32-02	0.0084	39829.41	47152.24	7322.83	598.01	98	6	Validação documentos
1536	197	71	2021-10-19 01:18:17-03	0.0087	115504.69	220508.17	105003.48	1816.57	93	4	Validação documentos
1706	790	71	2015-03-28 20:40:51-03	0.0186	89739.30	140752.04	51012.74	2920.05	46	1	Validação documentos
959	274	72	2016-11-28 13:42:34-02	0.0209	8101.37	13208.09	5106.72	1450.70	6	0	Validação documentos
963	328	72	2012-04-27 07:41:18-03	0.0104	134884.60	213373.63	78489.03	136287.40	1	5	Validação documentos
989	572	72	2014-03-21 03:34:00-03	0.0186	107061.69	155623.00	48561.31	2449.15	91	3	Validação documentos
1369	338	72	2013-08-30 08:18:10-03	0.0128	42079.17	81806.23	39727.06	1912.89	26	3	Validação documentos
1584	322	72	2014-12-30 23:15:41-02	0.0203	35841.42	64959.17	29117.75	863.51	92	6	Validação documentos
16	981	73	2020-11-23 07:51:35-03	0.0092	38057.26	74565.70	36508.44	1118.48	41	4	Validação documentos
138	490	73	2021-01-26 16:49:48-03	0.0105	7254.41	8976.72	1722.31	130.40	84	4	Validação documentos
579	407	73	2015-04-19 20:00:07-03	0.0115	90329.73	156474.35	66144.62	2196.74	56	2	Validação documentos
601	652	73	2017-08-25 22:16:31-03	0.0221	17368.03	21074.37	3706.34	437.49	96	5	Validação documentos
765	313	73	2012-08-08 09:16:18-03	0.0189	88232.11	162005.10	73772.99	5828.50	18	1	Validação documentos
947	140	73	2021-08-15 01:03:02-03	0.0105	137099.43	184068.72	46969.29	2501.99	82	2	Validação documentos
999	104	73	2015-11-08 17:24:08-02	0.0249	28303.16	53128.03	24824.87	1079.73	43	1	Validação documentos
1037	442	73	2022-01-20 02:30:48-03	0.0156	144531.28	188545.92	44014.64	5171.01	37	0	Validação documentos
1153	501	73	2020-05-11 21:10:40-03	0.0220	2353.70	3488.27	1134.57	69.40	63	3	Validação documentos
1588	889	73	2011-07-23 00:55:31-03	0.0146	19969.46	28148.71	8179.25	3500.37	6	0	Validação documentos
1667	684	73	2013-11-17 22:51:25-02	0.0120	118073.18	178655.96	60582.78	2710.79	62	6	Validação documentos
887	989	74	2020-06-23 01:56:07-03	0.0186	14742.67	18687.04	3944.37	328.12	98	0	Validação documentos
1825	430	74	2013-07-27 12:55:08-03	0.0188	191084.08	230874.08	39790.00	4070.35	115	3	Validação documentos
1993	765	74	2016-12-22 17:26:50-02	0.0161	156096.67	195335.64	39238.97	3138.54	101	1	Validação documentos
99	588	76	2019-04-06 03:48:25-03	0.0218	49638.64	98332.09	48693.45	1242.24	95	0	Validação documentos
489	42	76	2020-04-12 21:38:28-03	0.0120	17452.67	28326.94	10874.27	628.16	34	1	Validação documentos
538	457	76	2014-08-04 14:25:14-03	0.0100	149087.74	177171.70	28083.96	3690.82	52	2	Validação documentos
729	768	76	2014-09-11 02:29:49-03	0.0179	86828.39	107021.35	20192.96	1828.10	107	2	Validação documentos
806	433	76	2016-08-09 04:16:08-03	0.0105	86911.40	152189.73	65278.33	1665.59	76	1	Validação documentos
909	177	76	2016-12-24 07:40:10-02	0.0211	35356.98	61462.35	26105.37	1027.61	62	6	Validação documentos
1114	545	76	2014-05-29 02:01:57-03	0.0147	143079.42	182739.35	39659.93	3568.40	61	2	Validação documentos
1237	676	76	2022-08-21 18:19:10-03	0.0111	60455.78	115572.93	55117.15	1110.35	84	3	Validação documentos
1574	494	76	2017-06-29 12:17:51-03	0.0129	113387.00	151837.65	38450.65	3793.55	38	3	Validação documentos
1725	923	76	2019-07-31 23:38:40-03	0.0158	38494.16	47082.59	8588.43	845.78	81	3	Validação documentos
1973	893	76	2014-10-25 07:31:50-02	0.0115	107199.26	141694.47	34495.21	2789.98	51	3	Validação documentos
1982	531	76	2022-05-21 03:01:35-03	0.0203	86822.75	136106.33	49283.58	3002.67	44	3	Validação documentos
877	630	77	2020-04-22 16:12:03-03	0.0098	14004.70	18885.91	4881.21	213.01	106	4	Validação documentos
1270	501	77	2010-05-09 17:31:40-03	0.0089	113531.16	143216.91	29685.75	3317.09	41	5	Validação documentos
1471	196	77	2016-01-09 19:54:53-02	0.0088	162995.52	232447.69	69452.17	7859.15	23	5	Validação documentos
1606	471	77	2018-09-13 21:20:00-03	0.0243	100544.93	132337.01	31792.08	2635.70	109	3	Validação documentos
609	513	78	2010-03-10 00:53:20-03	0.0176	148922.93	226481.53	77558.60	38883.17	4	3	Validação documentos
751	387	78	2013-01-04 02:54:44-02	0.0179	10569.28	12704.21	2134.93	511.98	26	2	Validação documentos
1454	243	78	2010-02-16 15:52:27-02	0.0244	18572.26	33236.37	14664.11	1140.76	21	6	Validação documentos
80	767	79	2011-01-15 05:18:31-02	0.0158	15438.62	24044.68	8606.06	417.45	56	1	Validação documentos
1241	551	79	2018-12-12 20:42:39-02	0.0168	48477.52	59741.43	11263.91	1024.96	95	3	Validação documentos
287	841	80	2014-12-28 04:32:30-02	0.0103	23795.02	38379.64	14584.62	391.46	96	0	Validação documentos
380	151	80	2012-05-24 00:41:24-03	0.0202	24299.62	35011.19	10711.57	544.36	116	1	Validação documentos
1087	86	80	2011-02-21 09:57:02-03	0.0181	141117.54	168527.69	27410.15	3680.85	66	6	Validação documentos
1784	261	80	2011-07-19 01:20:27-03	0.0194	104959.12	150137.72	45178.60	2304.06	112	5	Validação documentos
112	379	81	2010-07-15 21:24:11-03	0.0147	4014.16	6914.30	2900.14	73.31	112	3	Validação documentos
146	666	81	2018-05-31 12:02:54-03	0.0211	163973.74	224240.67	60266.93	4304.30	78	5	Validação documentos
1143	681	81	2010-12-03 10:36:06-02	0.0206	61946.48	78942.57	16996.09	1408.37	116	0	Validação documentos
1203	788	81	2015-08-24 16:26:44-03	0.0190	15672.89	31093.89	15421.00	643.64	33	3	Validação documentos
1333	870	81	2017-11-03 07:46:58-02	0.0166	36158.12	68839.18	32681.06	769.40	92	6	Validação documentos
1778	154	81	2016-11-13 07:22:54-02	0.0154	59454.69	87337.04	27882.35	3089.41	23	5	Validação documentos
406	487	82	2017-04-29 13:23:53-03	0.0153	99008.81	196146.98	97138.17	1807.00	120	5	Validação documentos
1101	968	82	2015-01-13 13:50:20-02	0.0131	94186.22	138555.04	44368.82	5907.75	18	3	Validação documentos
1429	86	82	2010-12-31 02:58:17-02	0.0103	20872.37	29297.42	8425.05	324.50	106	3	Validação documentos
1881	96	82	2011-01-02 23:15:42-02	0.0096	163693.25	204658.91	40965.66	2391.81	112	5	Validação documentos
114	270	83	2021-04-07 14:48:28-03	0.0144	115692.95	226264.07	110571.12	2571.54	73	3	Validação documentos
808	854	83	2011-06-24 04:48:27-03	0.0178	88519.59	124590.29	36070.70	13561.91	7	3	Validação documentos
832	30	83	2015-11-02 12:38:39-02	0.0229	58703.80	116526.02	57822.22	1671.81	72	2	Validação documentos
1380	457	83	2013-11-30 16:37:30-02	0.0115	82770.02	155250.72	72480.70	1541.98	84	0	Validação documentos
1603	57	83	2012-11-10 03:55:13-02	0.0144	131949.64	165245.49	33295.85	3139.69	65	5	Validação documentos
1694	380	83	2016-12-01 21:53:05-02	0.0211	178018.87	246290.84	68271.97	4542.44	84	6	Validação documentos
616	617	84	2012-02-27 20:47:50-03	0.0120	113420.80	163020.72	49599.92	1887.86	107	4	Validação documentos
619	690	84	2018-11-16 14:27:39-02	0.0139	46260.25	68085.27	21825.02	1272.28	51	3	Validação documentos
783	168	84	2020-10-14 17:35:33-03	0.0167	68262.45	128315.08	60052.63	1602.81	75	4	Validação documentos
1157	648	84	2018-09-20 00:06:43-03	0.0240	78447.71	99704.19	21256.48	4799.45	21	0	Validação documentos
1377	901	84	2011-07-05 22:06:55-03	0.0150	1246.85	1825.43	578.58	42.46	39	5	Validação documentos
1498	756	84	2014-03-08 10:31:09-03	0.0129	53892.40	93705.03	39812.63	987.41	95	0	Validação documentos
1707	677	84	2013-09-23 23:11:16-03	0.0243	49442.53	67691.33	18248.80	1625.04	56	0	Validação documentos
1716	366	84	2014-06-28 02:06:57-03	0.0155	32782.29	60601.82	27819.53	605.17	119	4	Validação documentos
284	457	85	2013-07-16 04:56:00-03	0.0108	163357.67	226341.32	62983.65	3135.34	77	1	Validação documentos
533	693	85	2018-06-12 01:19:20-03	0.0132	5386.38	7743.10	2356.72	108.66	81	0	Validação documentos
618	545	85	2010-05-31 21:04:10-03	0.0194	68264.03	128761.57	60497.54	2391.32	42	6	Validação documentos
686	706	85	2019-02-11 02:30:07-02	0.0105	136282.76	176759.82	40477.06	2095.00	110	0	Validação documentos
914	300	85	2018-01-31 01:23:50-02	0.0194	4675.59	5526.89	851.30	193.17	33	3	Validação documentos
355	722	86	2011-10-17 06:25:40-02	0.0135	68082.72	104413.53	36330.81	3025.79	27	4	Validação documentos
612	76	86	2020-10-22 22:42:55-03	0.0118	75992.31	117110.56	41118.25	1862.09	56	6	Validação documentos
698	272	86	2020-08-17 17:22:36-03	0.0247	126164.56	178587.31	52422.75	3528.43	88	1	Validação documentos
1011	292	86	2022-09-05 23:25:29-03	0.0121	62126.61	121550.60	59423.99	1235.09	78	5	Validação documentos
1190	15	86	2014-07-23 08:54:56-03	0.0163	38683.04	69288.88	30605.84	1005.57	61	2	Validação documentos
1663	381	86	2020-07-13 16:37:25-03	0.0233	134122.58	193574.19	59451.61	4239.79	58	4	Validação documentos
1854	944	86	2010-12-04 22:29:37-02	0.0201	65194.68	85866.59	20671.91	3450.82	24	0	Validação documentos
195	750	87	2016-11-27 03:13:16-02	0.0244	24777.41	36468.82	11691.41	1645.20	19	4	Validação documentos
7	228	88	2012-05-18 05:24:05-03	0.0117	154751.66	247248.35	92496.69	9584.38	18	2	Validação documentos
252	972	88	2021-01-24 18:26:34-03	0.0187	107351.82	163689.48	56337.66	2583.53	81	2	Validação documentos
469	830	88	2020-10-19 11:33:10-03	0.0211	4356.74	8695.61	4338.87	147.03	47	4	Validação documentos
594	33	88	2016-01-12 12:52:18-02	0.0223	85889.83	127405.31	41515.48	4518.99	25	5	Validação documentos
774	322	88	2015-08-24 13:05:12-03	0.0100	1996.46	3976.20	1979.74	36.08	81	4	Validação documentos
1468	844	88	2014-04-03 02:15:42-03	0.0198	33564.50	57309.29	23744.79	827.05	83	0	Validação documentos
1575	743	88	2010-08-24 03:02:33-03	0.0234	55364.15	69378.04	14013.89	2146.49	40	4	Validação documentos
1598	913	88	2021-03-31 10:20:50-03	0.0196	77748.75	114567.75	36819.00	1953.75	78	6	Validação documentos
1674	984	88	2016-09-03 13:45:09-03	0.0179	1630.76	2345.51	714.75	47.89	53	6	Validação documentos
425	199	89	2022-05-27 21:14:13-03	0.0129	133660.49	191377.52	57717.03	4999.09	33	5	Validação documentos
546	44	89	2015-11-24 13:04:30-02	0.0226	110891.75	177956.23	67064.48	8336.36	16	3	Validação documentos
1358	48	89	2011-09-12 00:14:34-03	0.0205	108686.35	170052.23	61365.88	5599.69	25	6	Validação documentos
1553	818	89	2021-06-15 21:39:39-03	0.0225	118432.92	218435.54	100002.62	3177.19	82	2	Validação documentos
120	831	90	2018-06-01 02:30:49-03	0.0198	164172.77	216330.67	52157.90	3640.02	114	0	Validação documentos
243	492	90	2018-01-04 22:05:18-02	0.0124	37756.59	47969.59	10213.00	5664.63	7	0	Validação documentos
878	311	90	2011-05-22 16:45:36-03	0.0176	6725.52	9885.37	3159.85	135.01	120	1	Validação documentos
1582	601	90	2017-11-06 08:52:21-02	0.0144	81293.82	96923.58	15629.76	2549.05	43	6	Validação documentos
1786	683	90	2015-02-22 04:56:32-03	0.0114	31116.06	42654.46	11538.40	514.93	103	6	Validação documentos
158	261	91	2019-05-02 00:54:25-03	0.0131	47584.74	58186.96	10602.22	1362.32	47	3	Validação documentos
1816	478	91	2012-05-09 08:35:12-03	0.0216	57092.94	78253.63	21160.69	1366.22	109	0	Validação documentos
603	794	92	2012-10-18 22:55:46-03	0.0115	23008.32	36558.47	13550.15	1915.60	13	1	Validação documentos
749	758	92	2020-09-21 05:38:01-03	0.0131	27377.88	37637.52	10259.64	495.16	99	3	Validação documentos
811	324	92	2010-08-31 20:54:52-03	0.0217	32445.87	43567.91	11122.04	773.98	112	3	Validação documentos
883	729	92	2013-11-11 03:11:43-02	0.0098	55137.39	83213.64	28076.25	786.90	119	1	Validação documentos
1646	475	92	2021-07-16 20:07:48-03	0.0137	30553.57	38268.91	7715.34	804.37	54	0	Validação documentos
154	483	93	2015-10-03 17:23:11-03	0.0240	12881.78	23821.87	10940.09	348.48	92	0	Validação documentos
333	918	93	2020-05-13 01:36:10-03	0.0242	118352.14	149260.25	30908.11	4722.72	39	4	Validação documentos
805	698	93	2010-05-12 09:57:47-03	0.0093	81763.41	121363.92	39600.51	1990.26	52	2	Validação documentos
1374	47	93	2014-12-08 00:44:46-02	0.0157	86709.31	104296.15	17586.84	1678.26	107	5	Validação documentos
1446	295	93	2015-03-19 00:08:29-03	0.0088	143932.39	234909.33	90976.94	4916.60	34	6	Validação documentos
100	478	94	2014-02-01 00:50:28-02	0.0169	88908.87	145370.92	56462.05	3316.87	36	1	Validação documentos
444	433	94	2019-06-24 06:55:23-03	0.0090	87968.95	111303.42	23334.47	2006.76	56	0	Validação documentos
1452	294	94	2019-09-06 04:46:46-03	0.0081	109623.09	163655.78	54032.69	2314.09	60	4	Validação documentos
1473	85	94	2019-12-09 05:00:16-03	0.0239	14396.76	22385.06	7988.30	481.90	53	2	Validação documentos
1661	757	94	2013-03-23 05:16:09-03	0.0113	11049.09	15423.46	4374.37	229.26	70	6	Validação documentos
463	427	95	2015-03-26 17:28:29-03	0.0118	183196.50	241832.66	58636.16	21574.90	9	4	Validação documentos
544	648	95	2010-08-26 20:50:13-03	0.0134	195051.41	243678.11	48626.70	3277.10	120	5	Validação documentos
569	776	95	2021-04-20 03:01:44-03	0.0249	94036.99	165546.12	71509.13	2606.14	93	3	Validação documentos
635	511	95	2018-11-03 14:34:58-03	0.0162	106423.56	173705.88	67282.32	2212.54	94	6	Validação documentos
723	124	95	2011-08-23 20:35:16-03	0.0121	40588.94	71164.76	30575.82	767.09	85	2	Validação documentos
1	335	96	2022-11-23 15:21:09-03	0.0235	10408.07	16083.99	5675.92	265.72	109	3	Validação documentos
332	942	96	2020-09-12 20:24:18-03	0.0238	124255.83	154494.74	30238.91	9947.33	15	6	Validação documentos
362	263	96	2012-05-16 21:41:02-03	0.0093	54861.44	75906.15	21044.71	1009.99	76	3	Validação documentos
1488	789	96	2014-02-17 08:10:18-03	0.0142	77679.07	101179.58	23500.51	1405.23	109	1	Validação documentos
382	928	97	2017-05-20 18:35:28-03	0.0230	84581.20	114768.96	30187.76	4486.36	25	0	Validação documentos
880	322	97	2018-11-07 10:55:56-02	0.0129	38618.67	71664.02	33045.35	661.87	109	0	Validação documentos
1048	312	97	2014-01-21 20:10:34-02	0.0246	30306.62	39075.93	8769.31	1060.03	50	0	Validação documentos
1231	674	97	2016-02-07 18:39:14-02	0.0242	38157.03	65989.60	27832.57	6908.88	6	4	Validação documentos
1533	60	97	2019-08-25 17:05:55-03	0.0240	83633.70	158652.15	75018.45	43328.20	2	1	Validação documentos
1952	775	97	2019-04-20 00:40:57-03	0.0098	78536.04	147937.03	69400.99	1376.32	84	2	Validação documentos
412	522	98	2010-02-14 05:18:42-02	0.0126	112488.56	193436.68	80948.12	3743.50	38	5	Validação documentos
547	26	98	2010-05-09 09:35:14-03	0.0245	113770.58	177863.15	64092.57	3539.26	64	0	Validação documentos
655	253	98	2019-06-17 05:48:51-03	0.0216	83465.89	116437.84	32971.95	2474.97	61	6	Validação documentos
677	320	98	2014-06-30 05:48:48-03	0.0103	36538.57	44947.44	8408.87	829.50	59	6	Validação documentos
719	971	98	2012-05-06 07:49:26-03	0.0154	59659.42	71335.06	11675.64	1217.09	92	1	Validação documentos
904	361	98	2017-01-30 04:53:04-02	0.0145	151246.11	246575.12	95329.01	4673.80	44	1	Validação documentos
1002	532	98	2019-09-15 20:45:13-03	0.0143	119433.48	157959.53	38526.05	14237.18	9	2	Validação documentos
1188	579	98	2012-11-12 00:51:28-02	0.0227	53122.85	83791.95	30669.10	2653.24	27	4	Validação documentos
1210	723	98	2012-06-11 22:33:29-03	0.0157	42535.02	72631.63	30096.61	796.52	117	1	Validação documentos
1398	468	98	2019-09-29 00:45:17-03	0.0160	147330.92	202245.25	54914.33	3387.25	75	1	Validação documentos
1514	428	98	2013-12-23 02:44:33-02	0.0208	175681.77	222843.78	47162.01	5445.92	54	4	Validação documentos
15	568	99	2017-05-21 17:23:48-03	0.0161	40750.95	81348.38	40597.43	3499.24	13	2	Validação documentos
385	892	99	2021-09-11 16:12:42-03	0.0118	25373.32	31543.27	6169.95	638.05	54	5	Validação documentos
433	639	99	2017-01-04 17:49:00-02	0.0224	139890.90	225966.76	86075.86	21814.67	7	6	Validação documentos
850	259	99	2016-05-12 10:44:24-03	0.0146	164796.36	222119.13	57322.77	3417.45	84	5	Validação documentos
1758	176	99	2010-01-10 06:12:47-02	0.0133	96404.11	184043.36	87639.25	1673.39	110	6	Validação documentos
582	241	100	2019-02-25 02:03:22-03	0.0151	61292.83	82684.29	21391.46	2219.57	36	2	Validação documentos
970	106	100	2011-06-05 01:13:16-03	0.0225	100371.68	152272.50	51900.82	3482.00	47	2	Validação documentos
1361	814	100	2020-07-25 17:50:30-03	0.0227	45431.89	74490.36	29058.47	1326.03	67	5	Validação documentos
1562	998	100	2017-03-22 09:25:42-03	0.0147	114794.47	165036.26	50241.79	3505.12	45	0	Validação documentos
\.


--
-- Name: agencias agencias_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.agencias
    ADD CONSTRAINT agencias_pkey PRIMARY KEY (cod_agencia);


--
-- Name: clientes clientes_cpfcnpj_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.clientes
    ADD CONSTRAINT clientes_cpfcnpj_key UNIQUE (cpfcnpj);


--
-- Name: clientes clientes_email_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.clientes
    ADD CONSTRAINT clientes_email_key UNIQUE (email);


--
-- Name: clientes clientes_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.clientes
    ADD CONSTRAINT clientes_pkey PRIMARY KEY (cod_cliente);


--
-- Name: colaborador_agencia colaborador_agencia_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.colaborador_agencia
    ADD CONSTRAINT colaborador_agencia_pkey PRIMARY KEY (cod_colaborador, cod_agencia);


--
-- Name: colaboradores colaboradores_cpf_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.colaboradores
    ADD CONSTRAINT colaboradores_cpf_key UNIQUE (cpf);


--
-- Name: colaboradores colaboradores_email_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.colaboradores
    ADD CONSTRAINT colaboradores_email_key UNIQUE (email);


--
-- Name: colaboradores colaboradores_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.colaboradores
    ADD CONSTRAINT colaboradores_pkey PRIMARY KEY (cod_colaborador);


--
-- Name: contas contas_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.contas
    ADD CONSTRAINT contas_pkey PRIMARY KEY (num_conta);


--
-- Name: propostas_credito propostas_credito_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.propostas_credito
    ADD CONSTRAINT propostas_credito_pkey PRIMARY KEY (cod_proposta);


--
-- Name: colaborador_agencia colaborador_agencia_cod_agencia_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.colaborador_agencia
    ADD CONSTRAINT colaborador_agencia_cod_agencia_fkey FOREIGN KEY (cod_agencia) REFERENCES public.agencias(cod_agencia);


--
-- Name: colaborador_agencia colaborador_agencia_cod_colaborador_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.colaborador_agencia
    ADD CONSTRAINT colaborador_agencia_cod_colaborador_fkey FOREIGN KEY (cod_colaborador) REFERENCES public.colaboradores(cod_colaborador);


--
-- Name: contas contas_cod_agencia_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.contas
    ADD CONSTRAINT contas_cod_agencia_fkey FOREIGN KEY (cod_agencia) REFERENCES public.agencias(cod_agencia);


--
-- Name: contas contas_cod_cliente_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.contas
    ADD CONSTRAINT contas_cod_cliente_fkey FOREIGN KEY (cod_cliente) REFERENCES public.clientes(cod_cliente);


--
-- Name: contas contas_cod_colaborador_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.contas
    ADD CONSTRAINT contas_cod_colaborador_fkey FOREIGN KEY (cod_colaborador) REFERENCES public.colaboradores(cod_colaborador);


--
-- Name: propostas_credito propostas_credito_cod_cliente_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.propostas_credito
    ADD CONSTRAINT propostas_credito_cod_cliente_fkey FOREIGN KEY (cod_cliente) REFERENCES public.clientes(cod_cliente);


--
-- Name: propostas_credito propostas_credito_cod_colaborador_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.propostas_credito
    ADD CONSTRAINT propostas_credito_cod_colaborador_fkey FOREIGN KEY (cod_colaborador) REFERENCES public.colaboradores(cod_colaborador);


--
-- PostgreSQL database dump complete
--

