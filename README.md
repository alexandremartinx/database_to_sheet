# database_to_sheet

Este script conecta ao MySQL com connect_db, consulta vagas não escritas com get_unwritten_vagas, e as marca como escritas com mark_vagas_as_written. A classe Spreadsheet autentica via OAuth2, evita duplicatas com get_existing_rows_set, e adiciona dados à planilha com append_values.