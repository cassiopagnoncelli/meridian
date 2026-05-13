export class MeridianError extends Error {
  constructor(message: string) {
    super(message);
    this.name = new.target.name;
  }
}

export class MeridianDataError extends MeridianError {}

export class MeridianInputError extends MeridianError {}
