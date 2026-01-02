\## Diagrama de Entidade Relacionamento (DER)



```mermaid

erDiagram

&nbsp;   REPRES --{ FORNCLIEN  atende

&nbsp;   FORNCLIEN --{ PRODUTOS  fornece

&nbsp;   FORNCLIEN --{ PEDIDOS  realiza

&nbsp;   PEDIDOS --{ PEDIDOSITEM  contém

&nbsp;   PRODUTOS --{ PEDIDOSITEM  compõe



&nbsp;   REPRES {

&nbsp;       int CODREPRES PK

&nbsp;       string TIPOPESS

&nbsp;       string NOMEFAN

&nbsp;       float COMISSAOBASE

&nbsp;   }



&nbsp;   FORNCLIEN {

&nbsp;       int CODCLIFOR PK

&nbsp;       int TIPOCF

&nbsp;       int CODREPRES FK

&nbsp;       string NOMEFAN

&nbsp;       string CIDADE

&nbsp;       string UF

&nbsp;   }



&nbsp;   PRODUTOS {

&nbsp;       int CODPROD PK

&nbsp;       string NOMEPROD

&nbsp;       int CODFORNE FK

&nbsp;       float VALCUSTO

&nbsp;       float VALVENDA

&nbsp;       float QTDEESTQ

&nbsp;   }



&nbsp;   PEDIDOS {

&nbsp;       int NUMPED PK

&nbsp;       date DATAPPED

&nbsp;       int CODCLIEN FK

&nbsp;       float VALOR

&nbsp;       int SITUACAO

&nbsp;   }



&nbsp;   PEDIDOSITEM {

&nbsp;       int NUMPED PK, FK

&nbsp;       int NUMITEM PK

&nbsp;       int CODPROD FK

&nbsp;       float QTDE

&nbsp;       float VALUNIT

&nbsp;   }

