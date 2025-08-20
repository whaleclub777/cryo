extern crate proc_macro;

use proc_macro::TokenStream;
use proc_macro2::Span;
use quote::quote;
use syn::{parse_macro_input, parse_str, ItemStruct};

struct ToDataFramesMetaParams {
    flatten: Option<String>,
}

impl ToDataFramesMetaParams {
    fn parse_attributes(attrs: &[syn::Attribute]) -> syn::Result<Self> {
        let mut flatten = None;

        for attr in attrs.iter().filter(|a| a.path().is_ident("to_df")) {
            attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("flatten") {
                    let lit = meta.value()?.parse::<syn::LitStr>()?;
                    flatten = Some(lit.value());
                }
                Ok(())
            })?;
        }

        Ok(ToDataFramesMetaParams { flatten })
    }
}

/// implements ToDataFrames and ColumnData for struct
/// usage
/// ```no_run
/// #[derive(ToDataFrames)]
/// struct MyStruct {
///     n_rows: u64,
///     field1: Vec<u32>,
///     field2: Vec<String>,
///     #[to_df(flatten = "extract_others")]
///     others: BTreeMap<String, Vec<DynSolValue>>,
///     chain_id: Vec<u64>,
/// }
/// ```
#[proc_macro_derive(ToDataFrames, attributes(to_df))]
pub fn to_data_frames(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as ItemStruct);

    let name = &input.ident;

    let datatype = quote!(Datatype::#name);
    let datatype_str = name.to_string();

    let field_names_and_types: Vec<_> =
        input.fields.iter().map(|f| (f.ident.clone().unwrap(), f.ty.clone())).collect();

    let field_processing: Vec<_> = field_names_and_types
        .iter()
        .filter(|(name, _)| quote!(#name).to_string() != "n_rows")
        .filter(|(_, value)| quote!(#value).to_string().starts_with("Vec"))
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
            let field_name_str = quote!(#name).to_string();
            quote! {
                #macro_name!(cols, #field_name_str, self.#name, schema);
            }
        })
        .collect();

    let event_code = input
        .fields
        .iter()
        .find_map(|f| {
            // get path from #[to_df(flatten = "<path>")]
            let params = ToDataFramesMetaParams::parse_attributes(&f.attrs).unwrap();
            params.flatten.map(|s| (f, s))
        })
        .map(|(field, flatten)| {
            let expr = parse_str::<syn::Expr>(&flatten).unwrap();
            let field_name = field.ident.as_ref().unwrap();
            let field_name_str = field_name.to_string();
            quote! {
                #expr(&mut cols, #field_name_str, self.#field_name, self.n_rows as usize, schema);
            }
        });

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
