extern crate proc_macro;

use proc_macro::TokenStream;
use proc_macro2::Span;
use quote::quote;
use syn::{parse_macro_input, ItemStruct};

/// implements ToDataFrames and ColumnData for struct
#[proc_macro_attribute]
pub fn to_df(attrs: TokenStream, input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as ItemStruct);

    // parse input args
    let attrs = parse_macro_input!(attrs as syn::Meta);
    let syn::Meta::Path(datatype) = attrs else {
        panic!("Expected Meta::Path");
    };

    let name = &input.ident;

    let field_names_and_types: Vec<_> =
        input.clone().fields.into_iter().map(|f| (f.ident.unwrap(), f.ty)).collect();

    let field_processing: Vec<_> = field_names_and_types
        .iter()
        .filter(|(name, _)| format!("{}", quote!(#name)) != "n_rows")
        .filter(|(_, value)| format!("{}", quote!(#value)).starts_with("Vec"))
        .filter(|(name, _)| name != "chain_id")
        .map(|(name, ty)| {
            let macro_name = match quote!(#ty).to_string().as_str() {
                "Vec < Vec < u8 > >" => syn::Ident::new("with_column_binary", Span::call_site()),
                "Vec < Option < Vec < u8 > > >" => {
                    syn::Ident::new("with_column_binary", Span::call_site())
                }
                "Vec < U256 >" => syn::Ident::new("with_column_u256", Span::call_site()),
                "Vec < Option < U256 > >" => {
                    syn::Ident::new("with_column_option_u256", Span::call_site())
                }
                _ => syn::Ident::new("with_column", Span::call_site()),
            };
            let field_name_str = format!("{}", quote!(#name));
            quote! {
                #macro_name!(cols, #field_name_str, self.#name, schema);
            }
        })
        .collect();

    let has_event_cols = !field_names_and_types
        .iter()
        .filter(|(name, _)| name == "event_cols")
        .collect::<Vec<_>>()
        .is_empty();
    let event_code = if has_event_cols {
        // Generate the tokens for the event processing code
        quote! {
            let decoder = schema.log_decoder.clone();
            let u256_types: Vec<_> = schema.u256_types.clone().into_iter().collect();
            if let Some(decoder) = decoder {
                // Write columns even if there are no values decoded - indicates empty dataframe
                let chunk_len = self.n_rows;
                if self.event_cols.is_empty() {
                    for param in decoder.event.inputs.iter() {
                        let name = "event__".to_string() + param.name.as_str();
                        let name = PlSmallStr::from_string(name);
                        let ty = DynSolType::parse(&param.ty).unwrap();
                        let coltype = ColumnType::from_sol_type(&ty, &schema.binary_type).unwrap();
                        match coltype {
                            ColumnType::UInt256 => {
                                cols.extend(ColumnType::create_empty_u256_columns(&name, &u256_types, &schema.binary_type));
                            },
                            _ => {
                                cols.push(coltype.create_empty_column(&name));
                            },
                        }
                    }
                } else {
                    for (name, data) in self.event_cols {
                        let series_vec = decoder.make_series(
                            name,
                            data,
                            chunk_len as usize,
                            &u256_types,
                            &schema.binary_type,
                        );
                        match series_vec {
                            Ok(s) => {
                                cols.extend(s);
                            }
                            Err(e) => eprintln!("error creating frame: {}", e), /* TODO: see how best
                                                                                 * to
                                                                                 * bubble up error */
                        }
                    }
                }

                let drop_names = vec!["topic1".to_string(), "topic2".to_string(), "topic3".to_string(), "data".to_string()];
                cols.retain(|c| !drop_names.contains(&c.name().to_string()));
            }
        }
    } else {
        // Generate an empty set of tokens if has_event_cols is false
        quote! {}
    };

    fn map_type_to_column_type(ty: &syn::Type) -> Option<proc_macro2::TokenStream> {
        match quote!(#ty).to_string().as_str() {
            "Vec < bool >" => Some(quote! { ColumnType::Boolean }),
            "Vec < u32 >" => Some(quote! { ColumnType::UInt32 }),
            "Vec < u64 >" => Some(quote! { ColumnType::UInt64 }),
            "Vec < U256 >" => Some(quote! { ColumnType::UInt256 }),
            "Vec < i32 >" => Some(quote! { ColumnType::Int32 }),
            "Vec < i64 >" => Some(quote! { ColumnType::Int64 }),
            "Vec < f32 >" => Some(quote! { ColumnType::Float32 }),
            "Vec < f64 >" => Some(quote! { ColumnType::Float64 }),
            "Vec < String >" => Some(quote! { ColumnType::String }),
            "Vec < Vec < u8 > >" => Some(quote! { ColumnType::Binary }),

            "Vec < Option < bool > >" => Some(quote! { ColumnType::Boolean }),
            "Vec < Option < u32 > >" => Some(quote! { ColumnType::UInt32 }),
            "Vec < Option < u64 > >" => Some(quote! { ColumnType::UInt64 }),
            "Vec < Option < U256 > >" => Some(quote! { ColumnType::UInt256 }),
            "Vec < Option < i32 > >" => Some(quote! { ColumnType::Int32 }),
            "Vec < Option < i64 > >" => Some(quote! { ColumnType::Int64 }),
            "Vec < Option < f32 > >" => Some(quote! { ColumnType::Float32 }),
            "Vec < Option < f64 > >" => Some(quote! { ColumnType::Float64 }),
            "Vec < Option < String > >" => Some(quote! { ColumnType::String }),
            "Vec < Option < Vec < u8 > > >" => Some(quote! { ColumnType::Binary }),
            _ => None,
            // _ => quote! {ColumnType::Binary},
        }
    }

    let datatype_str =
        datatype.segments.iter().map(|seg| seg.ident.to_string()).collect::<Vec<_>>();
    let datatype_str = datatype_str.iter().last().unwrap();

    let mut column_types = Vec::new();
    for (name, ty) in field_names_and_types.iter() {
        if let Some(column_type) = map_type_to_column_type(ty) {
            let field_name_str = format!("{}", quote!(#name));
            column_types.push(quote! { (#field_name_str, #column_type) });
        } else if name != "n_rows" && name != "event_cols" {
            println!("invalid column type for {name} in table {datatype_str}");
        }
    }

    let expanded = quote! {
        #input

        impl ToDataFrames for #name {

            fn create_dfs(
                self,
                schemas: &std::collections::HashMap<Datatype, Table>,
                chain_id: u64,
            ) -> R<std::collections::HashMap<Datatype, DataFrame>> {
                let datatype = #datatype;
                let schema = schemas.get(&datatype).expect("schema not provided");
                let mut cols = Vec::with_capacity(schema.columns().len());

                #(#field_processing)*

                if self.chain_id.len() == 0 {
                    with_column!(cols, "chain_id", vec![chain_id; self.n_rows as usize], schema);
                } else {
                    with_column!(cols, "chain_id", self.chain_id, schema);
                }

                #event_code

                let df = DataFrame::new(cols).map_err(CollectError::PolarsError).sort_by_schema(schema)?;
                let mut output = std::collections::HashMap::new();
                output.insert(datatype, df);
                Ok(output)
            }
        }

        impl ColumnData for #name {

            fn column_types() -> indexmap::IndexMap<&'static str, ColumnType> {
                indexmap::IndexMap::from_iter(vec![
                    #(#column_types),*
                ])
            }
        }
    };

    expanded.into()
}
