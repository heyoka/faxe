%% Date: 20.04.17 - 20:45
%% Ⓒ 2017 Alexander Minichmair
-author("Alexander Minichmair").


-export_type([graph_definition/0, option_name/0, option_value/0, option_config/0]).

-type graph_definition() :: #{nodes => list(), edges => list()}.

-type option_name()              :: any            |
                                    is_set         |
                                    number         |
                                    integer        |
                                    float          |
                                    double         |
                                    string         |
                                    binary         |
                                    atom           |
                                    list           |
                                    lambda         |
                                    bool           |
                                    number_list    |
                                    integer_list   |
                                    float_list     |
                                    string_list    |
                                    binary_list    |
                                    atom_list      |
                                    lambda_list
.

-type option_any()               :: any().
-type option_is_set()            :: any().
-type option_number()            :: integer() | float().
-type option_integer()           :: integer().
-type option_float()             :: float().
-type option_double()            :: float().
-type option_string()            :: binary().
-type option_binary()            :: binary().
-type option_atom()              :: atom().
-type option_list()              :: list().
-type option_lambda()            :: fun().
-type option_bool()              :: true | false.
-type option_number_list()       :: list(option_number()).
-type option_integer_list()      :: list(option_integer()).
-type option_float_list()        :: list(option_float()).
-type option_string_list()       :: list(option_binary()).
-type option_binary_list()       :: option_string_list().
-type option_atom_list()         :: list(option_atom()).
-type option_lambda_list()       :: list(option_lambda()).
-type option_config()            :: {atom(), atom()}.



-type option_value()             :: option_any()            |
                                    option_is_set()         |
                                    option_number()         |
                                    option_integer()        |
                                    option_float()          |
                                    option_double()         |
                                    option_string()         |
                                    option_binary()         |
                                    option_atom()           |
                                    option_list()           |
                                    option_lambda()         |
                                    option_bool()           |
                                    option_number_list()    |
                                    option_integer_list()   |
                                    option_float_list()     |
                                    option_binary_list()    |
                                    option_atom_list()      |
                                    option_lambda_list().
