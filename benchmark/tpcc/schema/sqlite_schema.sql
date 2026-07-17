create table warehouse (w_id integer, w_name text, w_street_1 text, w_street_2 text, w_city text, w_state text, w_zip text, w_tax real, w_ytd real);
create table district (d_id integer, d_w_id integer, d_name text, d_street_1 text, d_street_2 text, d_city text, d_state text, d_zip text, d_tax real, d_ytd real, d_next_o_id integer);
create table customer (c_id integer, c_d_id integer, c_w_id integer, c_first text, c_middle text, c_last text, c_street_1 text, c_street_2 text, c_city text, c_state text, c_zip text, c_phone text, c_since text, c_credit text, c_credit_lim real, c_discount real, c_balance real, c_ytd_payment real, c_payment_cnt integer, c_delivery_cnt integer, c_data text);
create table history (h_c_id integer, h_c_d_id integer, h_c_w_id integer, h_d_id integer, h_w_id integer, h_date text, h_amount real, h_data text);
create table new_orders (no_o_id integer, no_d_id integer, no_w_id integer);
create table orders (o_id integer, o_d_id integer, o_w_id integer, o_c_id integer, o_entry_d text, o_carrier_id integer, o_ol_cnt integer, o_all_local integer);
create table order_line (ol_o_id integer, ol_d_id integer, ol_w_id integer, ol_number integer, ol_i_id integer, ol_supply_w_id integer, ol_delivery_d text, ol_quantity integer, ol_amount real, ol_dist_info text);
create table item (i_id integer, i_im_id integer, i_name text, i_price real, i_data text);
create table stock (s_i_id integer, s_w_id integer, s_quantity integer, s_dist_01 text, s_dist_02 text, s_dist_03 text, s_dist_04 text, s_dist_05 text, s_dist_06 text, s_dist_07 text, s_dist_08 text, s_dist_09 text, s_dist_10 text, s_ytd real, s_order_cnt integer, s_remote_cnt integer, s_data text);
