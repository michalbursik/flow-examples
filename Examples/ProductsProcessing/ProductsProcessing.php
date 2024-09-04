<?php

namespace Examples\ProductsProcessing;

use Flow\ETL\DataFrame;
use Flow\ETL\Filesystem\SaveMode;
use Flow\ETL\Join\Expression;

use function Flow\ETL\Adapter\CSV\to_csv;
use function Flow\ETL\Adapter\XML\from_xml;
use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\equal;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\when;

class ProductsProcessing
{
    public function run()
    {
        // The goal is to generate CSV that I can use to load into MySql database (LOAD INFILE).
        // I want to insert new or update existing products in the database.
        // Also, I would like to get some number of what items were inserted, updated or skipped
        // - (Not able to get it from "LOAD INFILE" directly)

        // My idea is to join IDs of existing products from DB to new products loaded from the XML file.
        // Ideally count the records with id present => that would be count of updates
        // and count the records with id not present => that would be count of inserts
        // and somehow extract from filters the skipped items count

        // There are also two tables that I am creating:
        // products (product 1)
        // product_variants (product 1, blue, 150 EUR)(product 1, red, 155 EUR)(...)
        // product_variants are all received products
        // product I am creating by grouping the records by "GROUPING_KEY" column

        $this->processProducts();

        // Here I am storing the products into the database (for simplification skipping)

        $this->processProductVariants();

        // Here once again I am storing product variants into the database (for simplification skipping)
    }

    private function processProducts(): void
    {
        $newProductsDF = data_frame()->read(from_xml('new_products.xml', 'products/product'));

        $newProductsDF->withEntries([
            'EXTERNAL_ID' => ref('node')->xpath('merchant_product_id')->domElementValue(),
            'GROUP_ID' => ref('node')->xpath('parent_product_id')->domElementValue(),
            'NAME' => ref('node')->xpath('product_name')->domElementValue(),
            'IMAGE_URL' => ref('node')->xpath('merchant_image_url')->domElementValue(),
            'PRICE' => ref('node')->xpath('search_price')->domElementValue(),
            'CURRENCY' => ref('node')->xpath('currency')->domElementValue(),
            'URL' => ref('node')->xpath('merchant_deep_link')->domElementValue(),

            // Temp column, just for filtering out the products
            'IN_STOCK' => when(
                ref('node')->xpath('in_stock')->domElementValue()->isIn(lit(['Y', '1', 'Yes'])),
                lit(true),
                lit(false)
            ),

            'BRAND' => ref('node')->xpath('brand_name')->domElementValue(),
            'DESCRIPTION' => ref('node')->xpath('description')->domElementValue(),

            // Static columns - filling up manually
            'SHOP_ID' => lit(1),
            'FEED_ID' => lit(1),
            'UPDATED' => lit(date('Y-m-d H:i:s')),

            // Grouping key
            'GROUPING_KEY' => ref('node')->xpath('merchant_product_id')->domElementValue(),
        ]);

        $this->applyFilters($newProductsDF);

        $newProductsDF->dropDuplicates('GROUPING_KEY');

        $existingProducts = require_once 'existing_products.php';
        $existingProductsDF = data_frame()->read(from_array($existingProducts));
        $newProductsDF->join($existingProductsDF, Expression::on(equal('GROUPING_KEY', 'GROUPING_KEY')));

        // Add the ID column based on the joined values -> ensures the products are updated instead of insert
        $newProductsDF->withEntry('ID',
            when(
                ref('JOINED_ID')->equals(lit('')),
                lit("NULL"),
                ref('JOINED_ID')
            )
        );

        $newProductsDF->drop('node', 'IN_STOCK', 'JOINED_ID', 'JOINED_GROUPING_KEY');
        $newProductsDF->saveMode(SaveMode::Overwrite);
        $newProductsDF->write(to_csv('output.csv', false));
        $newProductsDF->run();
    }

    private function processProductVariants(): void
    {
        $newProductVariantsDF = data_frame()->read(from_xml('new_products.xml', 'products/product'));

        $newProductVariantsDF->withEntries([
            'EXTERNAL_ID' => ref('node')->xpath('merchant_product_id')->domElementValue(),
            'GROUP_ID' => ref('node')->xpath('parent_product_id')->domElementValue(),
            'GTIN' => ref('node')->xpath('ean')->domElementValue(),
            'MPN' => ref('node')->xpath('model_number')->domElementValue(),
            'NAME' => ref('node')->xpath('product_name')->domElementValue(),
            'GENDER' => ref('node')->xpath('suitable_for')->domElementValue(),
            'IMAGE_URL' => ref('node')->xpath('merchant_image_url')->domElementValue(),
            'PRICE' => ref('node')->xpath('search_price')->domElementValue(),
            'CURRENCY' => ref('node')->xpath('currency')->domElementValue(),
            'SIZE' => ref('node')->xpath('size')->domElementValue(),
            'URL' => ref('node')->xpath('merchant_deep_link')->domElementValue(),
            'DIRECT_URL' => ref('node')->xpath('merchant_deep_link')->domElementValue(),
            'COLOR' => ref('node')->xpath('colour')->domElementValue(),

            // Temp column, just for filtering out the products
            'IN_STOCK' => when(
                ref('node')->xpath('in_stock')->domElementValue()->isIn(lit(['Y', '1', 'Yes'])),
                lit(true),
                lit(false)
            ),
            'BRAND' => ref('node')->xpath('brand_name')->domElementValue(),
            'DESCRIPTION' => ref('node')->xpath('description')->domElementValue(),

            'SHOP_ID' => lit(1),
            'FEED_ID' => lit(1),
            'UPDATED' => lit(date('Y-m-d H:i:s')),

            'GROUPING_KEY' => ref('node')->xpath('merchant_product_id')->domElementValue(),
        ]);

        $this->applyFilters($newProductVariantsDF);

        // Connect product with product_variants through product_id (database relation)
        $existingProductsAfterInsert = require_once 'existing_products_after_insert.php';

        $existingProductsDF = data_frame()->read(from_array($existingProductsAfterInsert));
        $newProductVariantsDF->join($existingProductsDF, Expression::on(equal('GROUPING_KEY', 'GROUPING_KEY')));
        $newProductVariantsDF->rename('JOINED_ID', 'ID');
        // Need to drop since joining columns needs to be unique.
        $newProductVariantsDF->drop('JOINED_GROUPING_KEY');

        $existingProductVariants = require_once 'existing_product_variants.php';
        $existingProductVariantsDF = data_frame()->read(from_array($existingProductVariants));
        $newProductVariantsDF->join($existingProductVariantsDF, Expression::on(equal('GROUPING_KEY', 'GROUPING_KEY')));

        // Add the ID column based on the joined values -> ensures the product variants are updated instead of insert
        $newProductVariantsDF->withEntry('ID',
            when(
                ref('JOINED_ID')->equals(lit('')),
                lit("NULL"),
                ref('JOINED_ID')
            )
        );

        $newProductVariantsDF->drop('node', 'IN_STOCK', 'JOINED_ID', 'JOINED_GROUPING_KEY');
        $newProductVariantsDF->saveMode(SaveMode::Overwrite);
        $newProductVariantsDF->write(to_csv('output.csv', false));
        $newProductVariantsDF->run();
    }

    private function getFilters(): array
    {
        return [
            // Required fields
            ref('node')->xpath('EXTERNAL_ID')->domElementValue()->notEquals(lit('')),
            ref('node')->xpath('NAME')->domElementValue()->notEquals(lit('')),
            ref('node')->xpath('URL')->domElementValue()->notEquals(lit('')),
            ref('node')->xpath('IMAGE_URL')->domElementValue()->notEquals(lit('')),

            // Available
            ref('node')->xpath('IN_STOCK')->domElementValue()->isTrue(),

            // Price higher than 0
            ref('node')->xpath('PRICE')->domElementValue()->greaterThan(lit(0)),
        ];
    }

    private function applyFilters(DataFrame $dataFrame): void
    {
        $filters = $this->getFilters();
        foreach ($filters as $filter) {
            $dataFrame->filter($filter);
        }
    }
}