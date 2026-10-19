export const ORGANIZATION_BRAND_COLOR_KEYS = [
	"primary",
	"secondary",
	"accent",
	"background",
] as const;

export type OrganizationBrandColorKey =
	(typeof ORGANIZATION_BRAND_COLOR_KEYS)[number];

export type OrganizationBrandColorSwatch = {
	key: OrganizationBrandColorKey;
	label: string;
	color: string;
};

export const EMPTY_BRAND_COLOR_SWATCHES: OrganizationBrandColorSwatch[] = [];
