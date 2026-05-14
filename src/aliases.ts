const COUNTRY_ALIASES = new Map<string, string>([
  ["brasil", "Brazil"],
  ["br", "Brazil"],
  ["gb", "United Kingdom"],
  ["uk", "United Kingdom"],
  ["u k", "United Kingdom"],
  ["great britain", "United Kingdom"],
  ["britain", "United Kingdom"],
  ["england", "United Kingdom"],
  ["tr", "Turkey"],
  ["turkiye", "Turkey"],
  ["turquia", "Turkey"],
  ["turquie", "Turkey"],
  ["turkei", "Turkey"],
  ["us", "United States"],
  ["u s", "United States"],
  ["usa", "United States"],
  ["u s a", "United States"],
  ["united states of america", "United States"],
  ["cd", "Democratic Republic of the Congo"],
  ["congo kinshasa", "Democratic Republic of the Congo"],
  ["dr congo", "Democratic Republic of the Congo"],
  ["drc", "Democratic Republic of the Congo"],
  ["cg", "Republic of the Congo"],
  ["congo brazzaville", "Republic of the Congo"],
  ["hk", "China"],
  ["hong kong sar china", "China"],
  ["ru", "Russia"],
  ["russian federation", "Russia"],
  ["kr", "South Korea"],
  ["korea republic of", "South Korea"],
  ["kp", "North Korea"],
  ["ir", "Iran"],
  ["iran islamic republic of", "Iran"],
  ["vn", "Vietnam"],
  ["viet nam", "Vietnam"],
  ["la", "Laos"],
  ["lao people s democratic republic", "Laos"],
  ["sy", "Syria"],
  ["syrian arab republic", "Syria"],
  ["bo", "Bolivia"],
  ["bolivia plurinational state of", "Bolivia"],
  ["tz", "Tanzania"],
  ["tanzania united republic of", "Tanzania"],
  ["md", "Moldova"],
  ["moldova republic of", "Moldova"],
  ["bn", "Brunei"],
  ["brunei darussalam", "Brunei"]
]);

const BRAZIL_STATE_ALIASES = new Map<string, string>([
  ["ac", "AC"],
  ["acre", "AC"],
  ["al", "AL"],
  ["alagoas", "AL"],
  ["ap", "AP"],
  ["amapa", "AP"],
  ["am", "AM"],
  ["amazonas", "AM"],
  ["ba", "BA"],
  ["bahia", "BA"],
  ["ce", "CE"],
  ["ceara", "CE"],
  ["df", "DF"],
  ["distrito federal", "DF"],
  ["es", "ES"],
  ["espirito santo", "ES"],
  ["go", "GO"],
  ["goias", "GO"],
  ["ma", "MA"],
  ["maranhao", "MA"],
  ["mt", "MT"],
  ["mato grosso", "MT"],
  ["ms", "MS"],
  ["mato grosso do sul", "MS"],
  ["mg", "MG"],
  ["minas gerais", "MG"],
  ["pa", "PA"],
  ["para", "PA"],
  ["pb", "PB"],
  ["paraiba", "PB"],
  ["pr", "PR"],
  ["parana", "PR"],
  ["pe", "PE"],
  ["pernambuco", "PE"],
  ["pi", "PI"],
  ["piaui", "PI"],
  ["rj", "RJ"],
  ["rio de janeiro", "RJ"],
  ["rn", "RN"],
  ["rio grande do norte", "RN"],
  ["rs", "RS"],
  ["rio grande do sul", "RS"],
  ["ro", "RO"],
  ["rondonia", "RO"],
  ["rr", "RR"],
  ["roraima", "RR"],
  ["sc", "SC"],
  ["santa catarina", "SC"],
  ["sp", "SP"],
  ["sao paulo", "SP"],
  ["se", "SE"],
  ["sergipe", "SE"],
  ["to", "TO"],
  ["tocantins", "TO"]
]);

export function canonicalCountry(value: string): string {
  const normalized = aliasKey(value);
  return COUNTRY_ALIASES.get(normalized) ?? value;
}

export function canonicalBrazilState(value: string): string {
  const normalized = aliasKey(value);
  return BRAZIL_STATE_ALIASES.get(normalized) ?? value;
}

function aliasKey(value: unknown): string {
  return String(value ?? "")
    .normalize("NFKD")
    .replace(/\p{Diacritic}/gu, "")
    .replace(/[^0-9A-Za-z]+/g, " ")
    .trim()
    .toLowerCase()
    .replace(/\s+/g, " ");
}
