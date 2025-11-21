mod download_fanout;

#[proc_macro_derive(
    FanoutConsumerGroup,
    attributes(fanout_consumer_group_error_ty, fanout_consumer_group_output_derive)
)]
pub fn derive_fanout_consumer_group(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let consumer_group = syn::parse_macro_input!(input as syn::DeriveInput);
    let mut error_ty = None;
    let mut fanout_consumer_group_output_derives = None;
    for attr in &consumer_group.attrs {
        if attr
            .meta
            .path()
            .get_ident()
            .is_some_and(|path_ident| path_ident == "fanout_consumer_group_error_ty")
        {
            error_ty = Some(attr.parse_args().unwrap());
        } else if attr
            .meta
            .path()
            .get_ident()
            .is_some_and(|path_ident| path_ident == "fanout_consumer_group_output_derive")
        {
            let syn::Meta::List(meta_list) = &attr.meta else {
                panic!(
                    "Expected #[fanout_consumer_group_output_derive(...)] to be a list of derives"
                )
            };
            let tokens = &meta_list.tokens;
            fanout_consumer_group_output_derives = Some(syn::parse_quote! {
                #[derive(#tokens)]
            });
        }
    }
    let error_ty =
        error_ty.expect("Error type must be specified with #[fanout_consumer_group_error_ty(...)]");
    let output_struct_ident = quote::format_ident!("{}Output", consumer_group.ident);
    let consumer_group_impl = download_fanout::consumer::impl_consumer_group(
        &consumer_group,
        &output_struct_ident,
        &error_ty,
    );
    let consumer_group_output_struct = download_fanout::consumer::create_consumer_group_output(
        consumer_group,
        output_struct_ident,
        fanout_consumer_group_output_derives,
    );
    let output = quote::quote! {
        #consumer_group_impl
        #consumer_group_output_struct
    }
    .into();
    output
}
